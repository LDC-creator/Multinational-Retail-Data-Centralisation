from database_utils import DatabaseConnector
from sqlalchemy import create_engine, inspect
import yaml
import pandas as pd
import tabula
import requests
import json
import numpy as np
import boto3
from io import StringIO
from urllib.parse import urlparse

class DataExtractor:

    def __init__(self):
        self.s3_client = boto3.client('s3')


    def list_number_of_stores(self, number_of_stores_endpoint, headers):
        """
        Retrieve the number of stores from the API.

        Args:
            number_of_stores_endpoint (str): The URL endpoint to retrieve the number of stores.
            headers (dict): Dictionary containing the headers required to access the API.

        Returns:
            int: Number of stores.
        """
        # try:
            # Send GET request to the API endpoint
        response = requests.get(number_of_stores_endpoint, headers=headers).content

        response = json.loads(response)

            # Check if the request was successful (status code 200)
        #     if response.status_code == 200:
        #         # Parse the JSON response and extract the number of stores
        #         number_of_stores = response.json().get('number_of_stores')
        #         return number_of_stores
        #     else:
        #         print(f"Failed to retrieve number of stores. Status code: {response.status_code}")
        #         return None
        # except requests.RequestException as e:
        #     print(f"Exception occurred: {e}")
        return response["number_stores"]
    
    def retrieve_stores_data(self, store_endpoint, headers, num_stores):
        """
        Retrieve data for all stores from the API and save it in a pandas DataFrame.

        Args:
            store_endpoint (str): The URL endpoint to retrieve store data.
            headers (dict): Dictionary containing the headers required to access the API.
            num_stores (int): Number of stores to retrieve.
            csv_filename (str): Name of the CSV file to save the data.

        Returns:
            pandas.DataFrame: DataFrame containing store data.
        """
        list_of_df = []

        for i in range(num_stores):
            try:
                # Send GET request to the API endpoint
                response = requests.get(f"{store_endpoint}{i}", headers=headers)

                # Check if the request was successful (status code 200)
                if response.status_code == 200:
                    # Parse the JSON response and convert it to a DataFrame
                    store_data = response.json()
                    df = pd.DataFrame(store_data, index=[np.nan])
                    list_of_df.append(df)
                else:
                    print(f"Failed to retrieve store data. Status code: {response.status_code}")
            except requests.RequestException as e:
                print(f"Exception occurred: {e}")

        # Concatenate all DataFrames in the list vertically
        final_df = pd.concat(list_of_df)

        # # Save DataFrame to a CSV file
        # final_df.to_csv(csv_filename, index=False)

        return final_df

    
    def list_db_tables(self,connector):
  
        inspector = inspect(connector)
        tables = inspector.get_table_names()
        return tables

    def read_rds_table(self, engine, table_name):
        query = f"SELECT * FROM {table_name}"
        df = pd.read_sql_query(query, engine)
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
    
    def extract_from_s3(self, s3_address, output_csv_path):
        try:
            # Parse the S3 address
            bucket_name, key = self.parse_s3_address(s3_address)
            
            # Get the object from the S3 bucket
            response = self.s3_client.get_object(Bucket=bucket_name, Key=key)
            # Read the CSV data from the object
            csv_data = response['Body'].read().decode('utf-8')
            
            # Convert the CSV data to a pandas DataFrame
            df = pd.read_csv(StringIO(csv_data))

            # Save the DataFrame to a CSV file
            df.to_csv(output_csv_path, index=False)
            
            return df
        except Exception as e:
            print("An error occurred:", e)

    def parse_s3_address(self, s3_address):
        # Remove "s3://" from the beginning of the address
        s3_address = s3_address.replace("s3://", "")
        # Split the address into bucket name and key
        parts = s3_address.split("/")
        bucket_name = parts[0]
        key = "/".join(parts[1:])
        return bucket_name, key
    


    def extract_json_from_s3(self,s3_url,file_name):
        
            bucket_name = 'data-handling-public'
            key = 'date_details.json'

            print(bucket_name)
            print(key)
            
            # Get the JSON object from the S3 bucket
            response = self.s3_client.get_object(Bucket=bucket_name, Key=key)
            # Read the JSON data from the object
            json_data = response['Body'].read().decode('utf-8')
            
            # Parse the JSON data
            data = pd.read_json(json_data)

            # Save the DataFrame to a CSV file
            data.to_csv(file_name, index=False)
            
            return data
    


# call and use function
# extractor = DataExtractor()
# file_name = "date_details_data.csv"
# json_data = extractor.extract_json_from_s3("https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json", file_name)
# print(json_data)



# connector = DatabaseConnector()


# Example usage
# extractor = DataExtractor()
# file_path = '/Users/User/MRDC/Multinational-Retail-Data-Centralisation/db_creds.yaml'
# engine = connector.init_db_engine(file_path)
# result_tables = extractor.list_db_tables(engine)
# print("Tables in the database:")
# for table in result_tables:
#     print(table)


# list the data in orders_table 
# defining the table name var 

# table_name = 'orders_table'

# # Call the read_rds_table method to extract data from the specified table
# data_frame = extractor.read_rds_table(engine, table_name)

# #confirming the DB is pandas DF
# print(type(data_frame))
# # Print the DataFrame
# print(f'This is the {table_name} database: {data_frame}')


