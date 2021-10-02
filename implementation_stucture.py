import psycopg2


# ------------ Manager (Model objects handler) ------------ #
class BaseManager:
    connection = None

    @classmethod
    def set_connection(cls, database_settings):
        connection = psycopg2.connect(**database_settings)
        connection.autocommit = True  # https://www.psycopg.org/docs/connection.html#connection.commit
        cls.connection = connection

    @classmethod
    def _get_cursor(cls):
        return cls.connection.cursor()

    @classmethod
    def _execute_query(cls, query, params=None):
        cursor = cls._get_cursor()
        cursor.execute(query, params)

    def __init__(self, model_class):
        self.model_class = model_class

    def select(self, *field_names, chunk_size=2000):
        # Build SELECT query
        fields_format = ', '.join(field_names)
        query = f"SELECT {fields_format} FROM {self.model_class.table_name}"

        # Execute query
        cursor = self._get_cursor()
        cursor.execute(query)

        # Fetch data obtained with the previous query execution
        # and transform it into `model_class` objects.
        # The fetching is done by batches of `chunk_size` to
        # avoid to run out of memory.
        model_objects = list()
        is_fetching_completed = False
        while not is_fetching_completed:
            result = cursor.fetchmany(size=chunk_size)
            for row_values in result:
                keys, values = field_names, row_values
                row_data = dict(zip(keys, values))
                model_objects.append(self.model_class(**row_data))
            is_fetching_completed = len(result) < chunk_size

        return model_objects

    def bulk_insert(self, rows: list):
        # Build INSERT query and params:
        field_names = rows[0].keys()
        assert all(row.keys() == field_names for row in rows[1:])  # confirm that all rows have the same fields

        fields_format = ", ".join(field_names)
        values_placeholder_format = ", ".join([f'({", ".join(["%s"] * len(field_names))})'] * len(rows))  # https://www.psycopg.org/docs/usage.html#passing-parameters-to-sql-queries

        query = f"INSERT INTO {self.model_class.table_name} ({fields_format}) " \
                f"VALUES {values_placeholder_format}"

        params = list()
        for row in rows:
            row_values = [row[field_name] for field_name in field_names]
            params += row_values

        # Execute query
        self._execute_query(query, params)

    def update(self, new_data: dict):
        # Build UPDATE query and params
        field_names = new_data.keys()
        placeholder_format = ', '.join([f'{field_name} = %s' for field_name in field_names])
        query = f"UPDATE {self.model_class.table_name} SET {placeholder_format}"
        params = list(new_data.values())

        # Execute query
        self._execute_query(query, params)

    def delete(self):
        # Build DELETE query
        query = f"DELETE FROM {self.model_class.table_name} "

        # Execute query
        self._execute_query(query)


# ----------------------- Model ----------------------- #
class MetaModel(type):
    manager_class = BaseManager

    def _get_manager(cls):
        return cls.manager_class(model_class=cls)

    @property
    def objects(cls):
        return cls._get_manager()


class BaseModel(metaclass=MetaModel):
    table_name = ""

    def __init__(self, **row_data):
        for field_name, value in row_data.items():
            setattr(self, field_name, value)

    def __repr__(self):
        attrs_format = ", ".join([f'{field}={value}' for field, value in self.__dict__.items()])
        return f"<{self.__class__.__name__}: ({attrs_format})>\n"


# ----------------------- Setup ----------------------- #
DB_SETTINGS = {
    'host': '127.0.0.1',
    'port': '5432',
    'database': 'ormify',
    'user': 'postgres',
    'password': 'postgresql123#'
}

BaseManager.set_connection(database_settings=DB_SETTINGS)


# ----------------------- Usage ----------------------- #
class Employee(BaseModel):
    manager_class = BaseManager
    table_name = "employees"


# SQL: SELECT first_name, last_name, salary, grade FROM employees;
employees = Employee.objects.select('first_name', 'last_name', 'salary', 'grade')  # employees: List[Employee]

print(f"First select result:\n {employees} \n")


# SQL: INSERT INTO employees (first_name, last_name, salary)
#  	VALUES ('Yan', 'KIKI', 10000), ('Yoweri', 'ALOH', 15000);
employees_data = [
    {"first_name": "Yan", "last_name": "KIKI", "salary": 10000},
    {"first_name": "Yoweri", "last_name": "ALOH", "salary": 15000}
]
Employee.objects.bulk_insert(rows=employees_data)

employees = Employee.objects.select('first_name', 'last_name', 'salary', 'grade')
print(f"Select result after bulk insert:\n {employees} \n")


# SQL: UPDATE employees SET salary = 17000, grade = 'L2';
Employee.objects.update(
    new_data={'salary': 17000, 'grade': 'L2'}
)

employees = Employee.objects.select('first_name', 'last_name', 'salary', 'grade')
print(f"Select result after update:\n {employees} \n")


# SQL: DELETE FROM employees;
Employee.objects.delete()

employees = Employee.objects.select('first_name', 'last_name', 'salary', 'grade')
print(f"Select result after delete:\n {employees} \n")