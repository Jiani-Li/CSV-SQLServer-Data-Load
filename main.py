import pandas as pd
import pyodbc

from exceptions import DatabaseError

class CsvToDatabase:

    SERVER_NAME = 'Jiani_da\MSSQLSERVER04'
    DATABASE_NAME = 'Python_SQL_CSV'
    DB_TABLE = 'master_list'
    CREATE_TABLE_QUERY = f'''CREATE TABLE [dbo].[{DB_TABLE}] ( 
    Date datetime,
    Server nvarchar(200),
    Cost float)'''
    INSERT_DATA_QUERY = f'''INSERT INTO [Python_SQL_CSV].[dbo].[{DB_TABLE}]
    (Date,Server,Cost)
    VALUES(?, ?, ?)'''
    CHECK_TABLE_QUERY = f"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{DB_TABLE}'"
    MAX_DATE_QUERY = f'SELECT MAX(Date) FROM [Python_SQL_CSV].[dbo].[{DB_TABLE}]'
    DEFAULT_MAX_DATE = '1900-01-01'
    REMOVE_DUPLICATES_QUERY = f'''WITH TMP AS (
        SELECT Date, Server, Cost, ROW_NUMBER()OVER(PARTITION BY Date, Server, Cost ORDER BY (SELECT NULL)) AS rn
        FROM [Python_SQL_CSV].[dbo].[{DB_TABLE}])
        DELETE FROM TMP WHERE rn > 1'''

    def __init__(self, original_file, new_file):
        self.original_file = original_file
        self.new_file = new_file

        self.conn = None
        self.cursor = None

    def db_connection(self):
        try:
            conn = pyodbc.connect(
                Trusted_Connection='Yes',
                Driver='{ODBC Driver 17 for SQL Server}',
                Server=self.SERVER_NAME,
                Database=self.DATABASE_NAME
            )
            cursor = conn.cursor()
            return conn, cursor
        except DatabaseError as e:
            print(e)

    def is_table_exist(self):
        self.cursor.execute(self.CHECK_TABLE_QUERY)
        count = self.cursor.fetchone()[0]
        return count > 0

    def create_db_table(self):
        if self.is_table_exist():
            return
        try:
            self.cursor.execute(self.CREATE_TABLE_QUERY)
            self.conn.commit()
            print('Table master_list has been created.')
        except DatabaseError as e:
            print(e)

    def insert_data(self, insert_data):
        for row in insert_data.itertuples():
            self.cursor.execute(
                self.INSERT_DATA_QUERY,
                row.Date,
                row.Server,
                row.Cost
            )
        self.conn.commit()

    def load_original_data(self):
        master_list = pd.read_csv(self.original_file, delimiter=';')
        master_list['Date'] = pd.to_datetime(master_list['Date'])
        try:
            self.insert_data(insert_data=master_list)
            print('Original Data has been loaded.')
        except DatabaseError as e:
            print(e)

    def get_max_date(self):
        self.cursor.execute(self.MAX_DATE_QUERY)
        max_date = self.cursor.fetchone()[0]
        if max_date is None:
            max_date = pd.to_datetime(self.DEFAULT_MAX_DATE)
        return max_date

    def load_incremental_data(self):
        incremental_list = pd.read_csv(self.new_file, delimiter=';')
        incremental_list['Date'] = pd.to_datetime(incremental_list['Date'])
        new_data = incremental_list[incremental_list['Date'] > self.get_max_date()]
        if new_data.empty:
            print('No new data to load.')
            return
        try:
            self.insert_data(insert_data=new_data)
            print('Incremental data has been loaded.')
        except DatabaseError as e:
            print(e)

    def remove_duplicates_db_data(self):
        try:
            self.cursor.execute(self.REMOVE_DUPLICATES_QUERY)
            self.conn.commit()
        except DatabaseError as e:
            print(e)

    def close_connection(self):
        self.conn.close()

    def run(self):
        self.conn, self.cursor = self.db_connection()
        self.create_db_table()
        self.load_original_data()
        self.load_incremental_data()
        self.remove_duplicates_db_data()
        self.close_connection()


csv_to_db = CsvToDatabase(original_file='2023 Jan data.csv', new_file='2023 Feb data.csv')
csv_to_db.run()
