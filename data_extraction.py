from database_utils import DatabaseConnector
from sqlalchemy import create_engine, MetaData
import yaml
import pandas as pd
import tabula

class DataExtractor:
    def read_db_creds(self, file_path):
        """
        Read database credentials from a YAML file.

        Args:
            file_path (str): Path to the YAML file containing credentials.

        Returns:
            dict: A dictionary containing the database credentials.
        """
        # try:
        with open(file_path, 'r') as file:
            creds = yaml.safe_load(file)
            print(creds)
            return creds
        # except FileNotFoundError:
        #     print(f"File {file_path} not found")
        #     return {}
        # except yaml.YAMLError as e:
        #     print(f"Error parsing YAML file: {e}")
        #     return {}

    def init_db_engine(self, file_path):
        creds = self.read_db_creds(file_path)
        RDS_HOST = creds['RDS_HOST']
        RDS_PASSWORD = creds['RDS_PASSWORD']
        RDS_USER = creds['RDS_USER']
        RDS_DATABASE = creds['RDS_DATABASE']
        RDS_PORT = creds['RDS_PORT']
        engine = create_engine(f"postgresql+psycopg2://{RDS_USER}:{RDS_PASSWORD}@{RDS_HOST}:{RDS_PORT}/{RDS_DATABASE}")
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
        # engine = connector.get_engine()
        
        # # Read the specified table into a DataFrame
        # query = f"SELECT * FROM {table_name}"
        df = pd.read_sql_table(table_name,connector)
        
        return df
    
    def retrieve_pdf_data(self, pdf_link):
        # Use tabula-py to read data from the PDF file
        tables = tabula.read_pdf(pdf_link, pages="all", multiple_tables=True)
        
        # Initialize an empty list to store DataFrames for each table
        dfs = []
        
        # Convert each table to a DataFrame and append to the list
        for table in tables:
            df = pd.DataFrame(table)
            dfs.append(df)
        
        # Concatenate all DataFrames in the list vertically
        pdf_df = pd.concat(dfs, ignore_index=True)
        
        return pdf_df



    # Create an instance of DataExtractor
extractor = DataExtractor()

# Path to the db_creds.yaml file
file_path = '/Users/User/MRDC/Multinational-Retail-Data-Centralisation/db_creds.yaml'

# List tables from the database and print the result
result_tables = extractor.list_db_tables(file_path)
print("Tables in the database:")
for table in result_tables:
    print(table)

# Assuming you have the table name containing user data
table_name = "legacy_users"

# Define the database URI
db_uri = "postgresql://postgres:Harvey16@localhost:5432/Pagila"

source_engine = extractor.init_db_engine(file_path)
# Create an instance of DatabaseConnector with the appropriate database URI
connector = DatabaseConnector(db_uri)

# Call the read_rds_table method to extract the specified table
data_frame = extractor.read_rds_table(source_engine, table_name)

# Print the DataFrame
print(data_frame)

# List tables from the database
tables = extractor.list_db_tables(file_path)

# link to pdf document(tabula data) 
pdf_link = "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"

pdf_doc = extractor.retrieve_pdf_data(pdf_link)

print (f"this is the pdf doc{pdf_doc}")


#java_export local file path

JAVA_HOME="/Library/Internet Plug-Ins/JavaAppletPlugin.plugin/Contents/Home"



# Loop through each table and extract data to a CSV file
# for table_name in tables:
#     # Call the read_rds_table method to extract the specified table
#     data_frame = extractor.read_rds_table(source_engine, table_name)
    
#     # Define the file name for the CSV file
#     csv_file_name = f"{table_name}.csv"
    
#     # Save the DataFrame to a CSV file
#     data_frame.to_csv(csv_file_name, index=False)