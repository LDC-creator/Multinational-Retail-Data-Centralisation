from utils.database_utils import DatabaseConnector
from utils.data_extraction import DataExtractor
from utils.data_cleaning import DataCleaning
import sqlalchemy
import pandas as pd

class Main:

    def __init__(self):
        self.connector = DatabaseConnector('/Users/User/MRDC/Multinational-Retail-Data-Centralisation/utils/db_creds.yaml')
        # Instantiate DataCleaning
        self.cleaner = DataCleaning()
        # Instantiate DataExtracor
        self.extractor = DataExtractor()


    def dim_users(self):


        # Call the engine variable
        db_engine = self.connector.engine
        # Call the read_rds_table method with necessary parameters
        table_name = 'legacy_users'  # Assuming you want to read users data
        users_data = self.extractor.read_rds_table(db_engine, table_name)

        
        # Call the clean_user_data method with the users data frame
        clean_user_data = self.cleaner.clean_user_data(users_data)


        # Call the upload to DB method with the cleaned data
        upload_to_db = self.connector.upload_to_db(clean_user_data,'dim_users')

        return upload_to_db


    def dim_card_details(self):

        csv_file_path = '/Users/User/MRDC/Multinational-Retail-Data-Centralisation/databases/dim_card_details.csv'
        # Call the engine variable
        db_engine = self.connector.engine
        
        # Read the CSV file into a DataFrame
        pdf_data = pd.read_csv(csv_file_path)


        # Call the clean_card_data method with the card data frame
        clean_card_data = self.cleaner.clean_card_data(pdf_data)

        # Call the upload to DB method with the cleaned data
        upload_to_db = self.connector.upload_to_db(clean_card_data, 'dim_card_details')

        return upload_to_db


    def dim_store_details(self):
        csv_file_path = '/Users/User/MRDC/Multinational-Retail-Data-Centralisation/databases/dim_store_details.csv'

        # Call the engine variable
        db_engine = self.connector.engine
        
        # Read the CSV file into a DataFrame
        store_data = pd.read_csv(csv_file_path)


        # Call the clean_card_data method with the card data frame
        clean_store_data = self.cleaner.clean_store_data(store_data)

        # Call the upload to DB method with the cleaned data
        upload_to_db = self.connector.upload_to_db(clean_store_data, 'dim_store_details')

        return upload_to_db 

    def dim_products(self):
        csv_file_path ='/Users/User/MRDC/Multinational-Retail-Data-Centralisation/databases/aws_products.csv'

        # Call the engine variable
        db_engine = self.connector.engine
        
        # Read the CSV file into a DataFrame
        product_data = pd.read_csv(csv_file_path)


        # Call the clean_card_data method with the card data frame
        clean_products_data = self.cleaner.clean_products_data(product_data)

        # Call the upload to DB method with the cleaned data
        upload_to_db = self.connector.upload_to_db(clean_products_data, 'dim_products')

        return upload_to_db 

    def orders_table(self):
        csv_file_path ='/Users/User/MRDC/Multinational-Retail-Data-Centralisation/databases/orders_table.csv'

        # Call the engine variable
        db_engine = self.connector.engine
        
        # Read the CSV file into a DataFrame
        orders_data = pd.read_csv(csv_file_path)


        # Call the clean_card_data method with the card data frame
        clean_orders_data = self.cleaner.clean_orders_data(orders_data)

        # Call the upload to DB method with the cleaned data
        upload_to_db = self.connector.upload_to_db(clean_orders_data, 'orders_table')
        
        return upload_to_db


    def dim_date_times(self):
        csv_file_path ='/Users/User/MRDC/Multinational-Retail-Data-Centralisation/databases/date_details_data.csv'

        # Call the engine variable
        db_engine = self.connector.engine
        
        # Read the CSV file into a DataFrame
        date_data = pd.read_csv(csv_file_path)


        # Call the clean_card_data method with the card data frame
        clean_date_details = self.cleaner.clean_date_details(date_data)

        # Call the upload to DB method with the cleaned data
        upload_to_db = self.connector.upload_to_db(clean_date_details, 'dim_date_times')
        
        return upload_to_db



if __name__ == "__main__":
    # Call dim_users method if this script is executed directly
    main_test = Main()
    main_test.dim_date_times()

