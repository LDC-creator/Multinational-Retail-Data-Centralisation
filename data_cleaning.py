import pandas as pd
from data_extraction import DataExtractor
import csv

class DataCleaning:
    def __init__(self, file_path):
        self.extractor = DataExtractor()
        self.source_engine = self.extractor.init_db_engine(file_path)
        self.table_name = "legacy_users"  # table name containing user data
        


    def clean_user_data(self, df):
        """
        Clean the user data by handling NULL values, errors with dates,
        incorrectly typed values, and rows filled with the wrong information.

        Args:
            df (pandas.DataFrame): DataFrame containing user data.

        Returns:
            pandas.DataFrame: Cleaned DataFrame.
        """
        # Drop rows with NULL values
        df = df.dropna()

        # Convert date columns to datetime format
        date_columns = ['date_of_birth', 'join_date']
        for col in date_columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')

        # Remove rows with invalid dates
        df = df.dropna(subset=date_columns)

        # Convert incorrectly typed values
        df['age'] = pd.to_numeric(df['age'], errors='coerce')

        # Remove rows with invalid ages (e.g., negative or too high)
        df = df[(df['age'] > 0) & (df['age'] < 150)]

        # Additional cleaning steps based on specific criteria

        # Return the cleaned DataFrame
        return df



# Path to the db_creds.yaml file
file_path = '/Users/User/MRDC/Multinational-Retail-Data-Centralisation/db_creds.yaml'
# Create an instance of DataExtractor
extractor = DataExtractor()

# Define df by reading data from the database
df = extractor.read_rds_table(extractor.init_db_engine(file_path), "legacy_users")

# Create an instance of DataCleaning
cleaner = DataCleaning(file_path)

# Clean the user data
cleaned_df = cleaner.clean_user_data(df)

# Now cleaned_df contains the cleaned user data
