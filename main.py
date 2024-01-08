import pandas as pd
import pyodbc

from exceptions import DatabaseError

class DatabaseVariables:
    def __init__(self):
        self.server_name = 'Jiani_da\MSSQLSERVER04'
        self.staging_db_name = 'Python_SQL_Staging'
        self.dw_db_name = 'Python_SQL_DW'

class FactDataVariables:

    def __init__(self):
        self.db_variables = DatabaseVariables()
        self.table_name = 'SalesOrder'
        self.create_table_query = f'''CREATE TABLE [dbo].[{self.table_name}](
        [SalesOrderID] [int] NOT NULL,
        [OrderDate] [datetime] NULL,
        [CustomerID] [int] NOT NULL,
        [ProductID] [int] NOT NULL,
        [UnitPrice] [money] NOT NULL,
        [OrderQty] [smallint] NOT NULL,
        [TaxAmt] [money] NULL,
        [TotalAmount] [money] NOT NULL
    )
   '''
        self.insert_data_query = f'''INSERT INTO [{self.db_variables.staging_db_name}].[dbo].[{self.table_name}]
        (SalesOrderID, OrderDate, CustomerID, ProductID, UnitPrice, OrderQty, TaxAmt, TotalAmount)
        VALUES(?, ?, ?, ?, ?, ?, ?, ?)'''
        self.check_table_query = f"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{self.table_name}'"
        self.max_date_query = f'SELECT MAX(OrderDate) FROM [{self.db_variables.staging_db_name}].[dbo].[{self.table_name}]'
        self.default_max_date = '1900-01-01'
        self.remove_duplicates_query = f'''WITH TMP AS (
        SELECT SalesOrderID, OrderDate, CustomerID, ProductID, UnitPrice, OrderQty, TaxAmt, TotalAmount, 
        ROW_NUMBER()OVER(PARTITION BY SalesOrderID, OrderDate, CustomerID, ProductID, UnitPrice, OrderQty, TaxAmt, TotalAmount 
        ORDER BY (SELECT NULL)) AS rn
        FROM [{self.db_variables.staging_db_name}].[dbo].[{self.table_name}])
        DELETE FROM TMP WHERE rn > 1'''

class DimensionDataVariables:
    def __init__(self):
        self.db_variables = DatabaseVariables()
        self.customer_table_name = 'Customer'
        self.product_table_name = 'Product'

        self.create_cust_table_query = f'''CREATE TABLE [{self.db_variables.staging_db_name}].[dbo].[{self.customer_table_name}](
        [CustomerID] [int] NOT NULL,
        [CompanyName] [varchar](200) NOT NULL,
        [SalesPerson] [varchar](300) NOT NULL)'''

        self.insert_cust_data_query = f'''INSERT INTO [{self.db_variables.staging_db_name}].[dbo].[{self.customer_table_name}]
        (CustomerID, CompanyName, SalesPerson)
        VALUES(?, ?, ?)'''
        self.create_product_table_query = f'''CREATE TABLE [{self.db_variables.staging_db_name}].[dbo].[{self.product_table_name}](
        [ProductID] [int] NOT NULL,
	    [ProductcategoryID] [int] NOT NULL,
	    [ProductName] [varchar](50) NOT NULL,
	    [ProductCategoryName] [varchar](50) NULL)'''
        self.insert_product_data_query = f'''INSERT INTO [{self.db_variables.staging_db_name}].[dbo].[{self.product_table_name}]
        (ProductID, ProductCategoryID, ProductName, ProductCategoryName)
        VALUES(?, ?, ?, ?)'''
        self.check_cust_table_query = f"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{self.customer_table_name}'"
        self.check_product_table_query = f"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{self.product_table_name}'"
        self.remove_duplicates_cust_query = f'''WITH TMP AS (
                SELECT CustomerID, CompanyName, SalesPerson, 
                ROW_NUMBER()OVER(PARTITION BY CustomerID, CompanyName, SalesPerson ORDER BY (SELECT NULL)) AS rn
                FROM [{self.db_variables.staging_db_name}].[dbo].[{self.customer_table_name}])
                DELETE FROM TMP WHERE rn > 1'''
        self.remove_duplicates_product_query = f'''WITH TMP AS (
                        SELECT ProductID, ProductCategoryID, ProductName, ProductCategoryName, 
                        ROW_NUMBER()OVER(PARTITION BY ProductID, ProductCategoryID, ProductName, ProductCategoryName 
                        ORDER BY (SELECT NULL)) AS rn
                        FROM [{self.db_variables.staging_db_name}].[dbo].[{self.product_table_name}])
                        DELETE FROM TMP WHERE rn > 1'''

class CsvHandler:
    def __init__(self, sales_header_csv, sales_detail_csv, customer_csv, product_csv, category_csv):
        self.sales_header_csv = sales_header_csv
        self.sales_detail_csv = sales_detail_csv
        self.customer_csv = customer_csv
        self.product_csv = product_csv
        self.category_csv = category_csv

        self.sales_header, self.sales_detail, self.customer, self.product, self.product_category = self.read_csv()
        self.sales_order = self.transform_sales()
        self.customer = self.transform_customer()
        self.sales_product = self.transform_product()

    def read_csv(self):
        sales_header = pd.read_csv(self.sales_header_csv, delimiter=',')
        sales_detail = pd.read_csv(self.sales_detail_csv, delimiter=',')
        customer = pd.read_csv(self.customer_csv, delimiter=',')
        product = pd.read_csv(self.product_csv, delimiter=',')
        product_category = pd.read_csv(self.category_csv, delimiter=',')
        return sales_header, sales_detail, customer, product, product_category

    def print_dataframe_columns(self):
        print(self.sales_header.columns, self.sales_detail.columns, self.customer.columns, self.product.columns,
              self.product_category.columns)

    def transform_sales(self):
        """
        Read SalesOrderHeader.csv and SalesOrderDetail.csv and load them into 2 data frame,
        keep the necessary columns and merge these 2 data frame.
        Transform some columns.
        """

        sales_header = self.sales_header[['SalesOrderID', 'OrderDate', 'CustomerID', 'TaxAmt']]
        sales_detail = self.sales_detail[['SalesOrderID', 'OrderQty', 'ProductID', 'UnitPrice']]
        sales_order = pd.merge(sales_header, sales_detail, on='SalesOrderID', how='inner')
        sales_order['OrderDate'] = pd.to_datetime(sales_order['OrderDate'])
        sales_order['TotalAmount'] = sales_order['UnitPrice'] * sales_order['OrderQty']
        return sales_order

    def transform_customer(self):
        """
        Read Customer.csv and load it into a data frame.
        keep the necessary columns and transform some columns.
        """
        customer = self.customer[['CustomerID', 'CompanyName', 'SalesPerson']]
        return customer

    def transform_product(self):
        """
        Read SalesOrderHeader.csv and SalesOrderDetail.csv and load them into 2 data frame,
        keep the necessary columns and merge these 2 data frame.
        Transform some columns.
        """
        product = self.product[['ProductID', 'Name', 'ProductCategoryID']].rename(columns={'Name': 'ProductName'})
        product_category = self.product_category[['ProductCategoryID', 'Name']].rename(columns={'Name': 'ProductCategoryName'})
        sales_product = pd.merge(product, product_category, on='ProductCategoryID', how='left')
        return sales_product

class DataFrameToStagingDb:
    def __init__(self, original_csv_handler, new_csv_handler):
        self.original_csv_handler = original_csv_handler
        self.new_csv_handler = new_csv_handler

        self.db_variables = DatabaseVariables()
        self.fact_data_variables = FactDataVariables()
        self.dim_data_variables = DimensionDataVariables()

        self.conn = None
        self.cursor = None

    def db_connection(self):
        try:
            conn = pyodbc.connect(
                Trusted_Connection='Yes',
                Driver='{ODBC Driver 17 for SQL Server}',
                Server=self.db_variables.server_name,
                Database=self.db_variables.staging_db_name
            )
            cursor = conn.cursor()
            return conn, cursor
        except DatabaseError as e:
            print(e)

    def is_cust_tables_exist(self):
        self.cursor.execute(self.dim_data_variables.check_cust_table_query)
        count_cust = self.cursor.fetchone()[0]
        return count_cust > 0

    def is_product_tables_exist(self):
        self.cursor.execute(self.dim_data_variables.check_product_table_query)
        count_product = self.cursor.fetchone()[0]
        return count_product > 0

    def is_fact_table_exist(self):
        self.cursor.execute(self.fact_data_variables.check_table_query)
        count = self.cursor.fetchone()[0]
        return count > 0

    def create_cust_tables(self):
        if self.is_cust_tables_exist():
            return
        try:
            self.cursor.execute(self.dim_data_variables.create_cust_table_query)
            self.conn.commit()
            print('Customer table has been created.')
        except DatabaseError as e:
            print(e)

    def create_product_tables(self):
        if self.is_product_tables_exist():
            return
        try:
            self.cursor.execute(self.dim_data_variables.create_product_table_query)
            self.conn.commit()
            print('Product table has been created.')
        except DatabaseError as e:
            print(e)

    def create_fact_table(self):
        if self.is_fact_table_exist():
            return
        try:
            self.cursor.execute(self.fact_data_variables.create_table_query)
            self.conn.commit()
            print('Sales order table has been created.')
        except DatabaseError as e:
            print(e)

    def insert_cust_data(self, insert_data):
        for row in insert_data.itertuples():
            self.cursor.execute(
                self.dim_data_variables.insert_cust_data_query,
                row.CustomerID,
                row.CompanyName,
                row.SalesPerson
            )
        self.conn.commit()

    def insert_product_data(self, insert_data):
        for row in insert_data.itertuples():
            self.cursor.execute(
                self.dim_data_variables.insert_product_data_query,
                row.ProductID,
                row.ProductCategoryID,
                row.ProductName,
                row.ProductCategoryName
            )
        self.conn.commit()

    def insert_fact_data(self, insert_data):
        for row in insert_data.itertuples():
            self.cursor.execute(
                self.fact_data_variables.insert_data_query,
                row.SalesOrderID,
                row.OrderDate,
                row.CustomerID,
                row.ProductID,
                row.UnitPrice,
                row.OrderQty,
                row.TaxAmt,
                row.TotalAmount
            )
        self.conn.commit()

    def load_original_data(self, csv_handler):
        sales_order = csv_handler.transform_sales()
        try:
            self.insert_fact_data(insert_data=sales_order)
            print('Original Data has been loaded.')
        except DatabaseError as e:
            print(e)

    def get_max_date(self):
        self.cursor.execute(self.fact_data_variables.max_date_query)
        max_date = self.cursor.fetchone()[0]
        if max_date is None:
            max_date = pd.to_datetime(self.fact_data_variables.default_max_date)
        return max_date

    def load_incremental_data(self, incremental_csv_handler):
        incremental_order = incremental_csv_handler.transform_sales()
        new_data = incremental_order[incremental_order['OrderDate'] > self.get_max_date()]
        if new_data.empty:
            print('No new sales data to load.')
            return
        try:
            self.insert_fact_data(insert_data=new_data)
            print('Incremental data has been loaded.')
        except DatabaseError as e:
            print(e)

    def load_dim_data(self, csv_handler):
        customer = csv_handler.transform_customer()
        product = csv_handler.transform_product()
        try:
            self.insert_cust_data(insert_data=customer)
            self.insert_product_data(insert_data=product)
            print('Dimension Data has been loaded.')
        except DatabaseError as e:
            print(e)

    def remove_duplicates_db_data(self):
        try:
            self.cursor.execute(self.fact_data_variables.remove_duplicates_query)
            self.cursor.execute(self.dim_data_variables.remove_duplicates_cust_query)
            self.cursor.execute(self.dim_data_variables.remove_duplicates_product_query)
            self.conn.commit()
        except DatabaseError as e:
            print(e)

    def close_connection(self):
        self.conn.close()

    def run(self):
        self.conn, self.cursor = self.db_connection()
        self.create_fact_table()
        self.create_cust_tables()
        self.create_product_tables()
        self.load_original_data(csv_handler)
        self.load_incremental_data(incremental_csv_handler)
        self.load_dim_data(csv_handler)
        self.remove_duplicates_db_data()
        self.close_connection()

csv_handler = CsvHandler('SalesOrderHeader2020.csv', 'SalesOrderDetail2020.csv', 'Customer.csv', 'Product.csv', 'ProductCategory.csv')
incremental_csv_handler = CsvHandler('SalesOrderHeader2021.csv', 'SalesOrderDetail2021.csv', 'Customer.csv', 'Product.csv', 'ProductCategory.csv')
csv_to_db = DataFrameToStagingDb(csv_handler, incremental_csv_handler)
csv_to_db.run()



