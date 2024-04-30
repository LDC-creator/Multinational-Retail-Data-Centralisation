from database_utils import DatabaseConnector
from data_extraction import DataExtractor
from data_cleaning import DataCleaning
import sqlalchemy

class Main:

    @staticmethod
    def dim_users():
        # Instantiate DatabaseConnector
        connector = DatabaseConnector()

        # Call the read_db_creds method
        read_db_creds = connector.read_db_creds('/Users/User/MRDC/Multinational-Retail-Data-Centralisation/db_creds.yaml')

        # Call the init_db_engine method
        db_engine = connector.init_db_engine('/Users/User/MRDC/Multinational-Retail-Data-Centralisation/db_creds.yaml')

        # Instantiate DataExtracor
        extractor = DataExtractor()

        # Call the read_rds_table method with necessary parameters
        table_name = 'legacy_users'  # Assuming you want to read users data
        users_data = extractor.read_rds_table(db_engine, table_name)

        # Instantiate DataCleaning
        cleaner = DataCleaning()

        # Call the clean_user_data method with the users data frame
        clean_user_data = cleaner.clean_user_data(users_data)

        # Call the upload to DB method with the cleaned data
        upload_to_db = connector.upload_to_db(clean_user_data)

        return upload_to_db

    @staticmethod
    def dim_card_details(): 
        pass 

    @staticmethod
    def dim_store_details():
        pass 

    @staticmethod
    def dim_products():
        pass 

    @staticmethod
    def orders_table():
        pass 

    @staticmethod
    def dim_date_times():
        pass


if __name__ == "__main__":
    # Call dim_users method if this script is executed directly
    Main.dim_users()
