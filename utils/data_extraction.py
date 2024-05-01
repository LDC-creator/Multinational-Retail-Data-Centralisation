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
import os

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
    
        response = requests.get(number_of_stores_endpoint, headers=headers).content

        response = json.loads(response)

        return response["number_stores"]
    
    def retrieve_stores_data(self, store_endpoint, headers, num_stores, csv_filename="dim_store_details.csv"):
        """
        Retrieve data for all stores from the API and save it in a pandas DataFrame.

        Args:
            store_endpoint (str): The URL endpoint to retrieve store data.
            headers (dict): Dictionary containing the headers required to access the API.
            num_stores (int): Number of stores to retrieve.
            csv_filename (str, optional): Name of the CSV file to save the data. Defaults to "dim_store_details.csv".

        Returns:
            pandas.DataFrame: DataFrame containing store data.
        """
        # Check if the CSV file already exists
        if os.path.isfile(csv_filename):
            print(f"CSV file '{csv_filename}' already exists. Skipping data retrieval.")
            return pd.read_csv(csv_filename)

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

        # Save DataFrame to a CSV file if it doesn't exist
        if not os.path.isfile(csv_filename):
            final_df.to_csv(csv_filename, index=False)

        return final_df

    
    def list_db_tables(self,connector):
  
        inspector = inspect(connector)
        tables = inspector.get_table_names()
        return tables

    def read_rds_table(self, engine, table_name):
        query = f"SELECT * FROM {table_name}"
        df = pd.read_sql_query(query, engine)
        return df



    
    def retrieve_pdf_data(self):

        pdf_link = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf'

        #determaning the output path to CSV file
        output_csv_path = '/Users/User/MRDC/Multinational-Retail-Data-Centralisation/databases/dim_card_details.csv'
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


        if os.path.exists(output_csv_path):
            print(f"A file with the name '{output_csv_path}' already exists. Not saving the new file.")
        else:
            # Save the DataFrame to a CSV file
            pdf_df.to_csv(output_csv_path, index=False)
        
        return pdf_df
    
    def extract_from_s3(self, output_csv_path):
        """
        Extract data from an S3 bucket and save it to a CSV file.

        Args:
            output_csv_path (str): Path to the output CSV file.

        Returns:
            pandas.DataFrame: DataFrame containing the extracted data.
        """
        # Check if the output CSV file already exists
        if os.path.exists(output_csv_path):
            print(f"CSV file already exists at {output_csv_path}. Skipping extraction.")
            return pd.read_csv(output_csv_path)
        
        try:
            # Parse the S3 address
            bucket_name = 'data-handling-public'
            key = 'products.csv'
            
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


    def extract_json_from_s3(self,file_name):
        """
        Extract JSON data from an S3 bucket and save it to a CSV file.

        Args:
            s3_url (str): URL of the S3 object.
            file_name (str): Path to the output CSV file.

        Returns:
            pandas.DataFrame: DataFrame containing the extracted JSON data.
        """
        # Check if the output CSV file already exists
        if os.path.exists(file_name):
            print(f"CSV file already exists at {file_name}. Skipping extraction.")
            return pd.read_csv(file_name)
        
        try:
            # Parse the S3 address
            bucket_name = 'data-handling-public'
            key = 'date_details.json'

            # Get the JSON object from the S3 bucket
            response = self.s3_client.get_object(Bucket=bucket_name, Key=key)
            
            # Read the JSON data from the object
            json_data = response['Body'].read().decode('utf-8')
            
            # Parse the JSON data
            data = pd.read_json(json_data)

            # Save the DataFrame to a CSV file
            data.to_csv(file_name, index=False)
            
            return data
        except Exception as e:
            print("An error occurred:", e)
        


