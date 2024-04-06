from sqlalchemy import create_engine, MetaData
import yaml
import pandas as pd

class DataExtractor:
    def read_db_creds(self, file_path):
        """
        Read database credentials from a YAML file.

        Args:
            file_path (str): Path to the YAML file containing credentials.

        Returns:
            dict: A dictionary containing the database credentials.
        """
        try:
            with open(file_path, 'r') as file:
                creds = yaml.safe_load(file)
                return creds
        except FileNotFoundError:
            print(f"File '{file_path}' not found.")
            return {}
        except yaml.YAMLError as e:
            print(f"Error parsing YAML file: {e}")
            return {}

    def init_db_engine(self, file_path):
        creds = self.read_db_creds(file_path)
        RDS_HOST = creds['RDS_HOST']
        RDS_PASSWORD = creds['RDS_PASSWORD']
        RDS_USER = creds['RDS_USER']
        RDS_DATABASE = creds['RDS_DATABASE']
        RDS_PORT = creds['RDS_PORT']
        engine = create_engine(f"postgresql://{RDS_USER}:{RDS_PASSWORD}@{RDS_HOST}:{RDS_PORT}/{RDS_DATABASE}")
        return engine

    def list_db_tables(self, file_path):
        # Establish a connection to the database
        engine = self.init_db_engine(file_path)
        
        # Create a MetaData object
        meta = MetaData()
        
        # Reflect metadata from the database
        meta.reflect(bind=engine)
        
        # Get the list of table names
        tables = meta.tables.keys()
        
        return tables

    def read_rds_table(self, connector, table_name):
        """
        Extracts the specified database table to a pandas DataFrame.

        Args:
            connector (DatabaseConnector): An instance of DatabaseConnector class.
            table_name (str): Name of the database table to extract.

        Returns:
            pandas.DataFrame: DataFrame containing the extracted data.
        """
        # Establish a connection to the database using DatabaseConnector
        engine = connector.get_engine()
        
        # Read the specified table into a DataFrame
        query = f"SELECT * FROM {table_name}"
        df = pd.read_sql(query, engine)
        
        return df

# Create an instance of DataExtractor
extractor = DataExtractor()

# Path to the db_creds.yaml file
file_path = '/Users/User/MRDC/Multinational-Retail-Data-Centralisation/db_creds.yaml'

# List tables from the database and print the result
result_tables = extractor.list_db_tables(file_path)
print("Tables in the database:")
for table in result_tables:
    print(table)

# Assuming you have an instance of DatabaseConnector named 'connector'
# and you have the table name containing user data
table_name = "legacy_users"

# Call the read_rds_table method to extract the specified table
data_frame = extractor.read_rds_table(connector, table_name)

# Print the DataFrame
print(data_frame)
