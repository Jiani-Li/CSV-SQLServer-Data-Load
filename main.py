import pandas as pd
import pyodbc

def database_connect(server, database):
    try:
        conn = pyodbc.connect(
        Trusted_Connection='Yes',
        Driver='{ODBC Driver 17 for SQL Server}',
        Server=server,
        Database=database
        )
        cursor = conn.cursor()
        return conn, cursor

    except Exception as e:
        print(f'Error connecting to the database: {e}')
        return None, None

def create_dbtable(original_file,cursor, conn):
    try:
        # Read the original csv data
        masterlist = pd.read_csv(original_file, delimiter=';')
        masterlist['Date'] = pd.to_datetime(masterlist['Date'])
        #Create table in database
        cursor.execute('''
        CREATE TABLE masterlist (
            Date datetime,
            Server nvarchar(200),
            Cost float
            )
        ''')
        conn.commit()
        print('Table is Created!')

    except Exception as e:
        print(f'Error creating table: {e}')

def load_original_data(original_file, cursor, conn):
    try:
        masterlist = pd.read_csv(original_file, delimiter=';')
        for row in masterlist.itertuples():
            cursor.execute(
                '''
                INSERT INTO [Python-SQL-CSV].[dbo].[masterlist]
                (Date,Server,Cost)
                VALUES(?, ?, ?)
                ''',
                row.Date,
                row.Server,
                row.Cost
            )
        conn.commit()
        print('Original Data is loaded.')

    except Exception as e:
        print(f"Error loading data: {e}")

def load_incremental_data(new_file, cursor, conn):
    try:
        incremental_list = pd.read_csv(new_file, delimiter=';')
        incremental_list['Date'] = pd.to_datetime(incremental_list['Date'])
        cursor.execute('SELECT MAX(Date) FROM [Python-SQL-CSV].[dbo].[masterlist]')
        max_date = cursor.fetchone()[0]

        # If there is no data in the database, set a minium date as 1900-01-01
        if max_date is None:
            max_date = pd.to_datetime('1900-01-01')

        # Filter out incremental data with dates greater than the maximum date in the database
        new_data = incremental_list[incremental_list['Date'] > max_date]

        # If there is new_data, insert the new_data into database
        if not new_data.empty:
            for row in new_data.itertuples():
                cursor.execute(
                    '''
                    INSERT INTO [Python-SQL-CSV].[dbo].[masterlist]
                    (Date,Server,Cost)
                    VALUES(?, ?, ?)
                    ''',
                    row.Date,
                    row.Server,
                    row.Cost
                    )
                conn.commit()
            print('Incremental data is loaded.')
        else:
                print('No new data to load.')

    except Exception as e:
        print(f'Error loading incremental data: {e}')


server_name = 'Jiani_da\MSSQLSERVER04'
database_name = 'Python-SQL-CSV'
original_file = '2023 Jan data.csv'
new_file = '2023 Mar data.csv'
# Calling the function to connect to the database
conn, cursor = database_connect(server_name, database_name)
# Calling the function to create table in database
#create_dbtable(original_file, cursor, conn)
# Calling the function to load the original data
#load_original_data(original_file, cursor, conn)
# Calling the function to load the incremental data
load_incremental_data(new_file, cursor, conn)
# Close the connection to the database
conn.close()
