import pandas as pd
import pyodbc

class CsvToDatabase:

    SERVER_NAME = 'Jiani_da\MSSQLSERVER04'
    DATABASE_NAME = 'Python-SQL-CSV'
    CREATE_TABLE_QUERY = f'''CREATE TABLE master_list ( 
    Date datetime,
    Server nvarchar(200),
    Cost float)'''
    INSERT_DATA_QUERY = f'''INSERT INTO [Python-SQL-CSV].[dbo].[master_list]
    (Date,Server,Cost)
    VALUES(?, ?, ?)'''

    def __init__(self, original_file, new_file):
        self.original_file = original_file
        self.new_file = new_file

        self.conn = None
        self.cursor = None

    def db_connection(self):
        conn = pyodbc.connect(
                Trusted_Connection='Yes',
                Driver='{ODBC Driver 17 for SQL Server}',
                Server=self.SERVER_NAME,
                Database=self.DATABASE_NAME
            )
        cursor = conn.cursor()
        return conn, cursor

    def is_table_exist(self, table_name):
        check_table_query = f'SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = "table_name"'
        self.cursor.execute(check_table_query)
        count = self.cursor.fetchone()[0]
        return count > 0

    def create_db_table(self):
        if self.is_table_exist(table_name='master_list'):
            return
        self.cursor.execute(self.CREATE_TABLE_QUERY)
        self.conn.commit()
        print('Table master_list has been created!')

    def load_original_data(self):
        master_list = pd.read_csv(self.original_file, delimiter=';')
        master_list['Date'] = pd.to_datetime(master_list['Date'])

        for row in master_list.itertuples():
            self.cursor.execute(
                self.INSERT_DATA_QUERY,
                row.Date,
                row.Server,
                row.Cost
            )
        self.conn.commit()
        print('Original Data has been loaded.')

    def load_incremental_data(self):
        incremental_list = pd.read_csv(self.new_file, delimiter=';')
        incremental_list['Date'] = pd.to_datetime(incremental_list['Date'])
        max_date_query = f'SELECT MAX(Date) FROM [Python-SQL-CSV].[dbo].[master_list]'
        self.cursor.execute(max_date_query)
        max_date = self.cursor.fetchone()[0]
        if max_date is None:
            max_date = pd.to_datetime('1900-01-01')

        new_data = incremental_list[incremental_list['Date'] > max_date]

        if new_data.empty:
            print('No new data to load.')
            return

        for row in new_data.itertuples():
            self.cursor.execute(
                self.INSERT_DATA_QUERY,
                row.Date,
                row.Server,
                row.Cost
            )
        self.conn.commit()
        print('Incremental data has been loaded.')

    def close_connection(self):
        self.conn.close()

    def run(self):
        self.conn, self.cursor = self.db_connection()
        self.create_db_table()
        self.load_original_data()
        self.load_incremental_data()
        self.close_connection()

csv_to_db = CsvToDatabase(original_file='2023 Jan data.csv', new_file='2023 Feb data.csv')
csv_to_db.run()
