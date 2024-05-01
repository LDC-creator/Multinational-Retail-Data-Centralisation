import pandas as pd
import psycopg2
import sqlalchemy
import re


class DataCleaning:

    def __init__(self):
        pass


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
            pd.options.mode.chained_assignment = None 
            df[col] = pd.to_datetime(df[col], errors='coerce')

        # Remove rows with invalid dates
        df = df.dropna(subset=date_columns)

        # If 'age' column exists, convert incorrectly typed values and remove rows with invalid ages
        if 'age' in df.columns:
            df['age'] = pd.to_numeric(df['age'], errors='coerce')
            #Remove invalid ages if present
            df = df[(df['age'] > 0) & (df['age'] < 150)]


        # Additional cleaning steps based on specific criteria

        # Return the cleaned DataFrame
        return df
    

    def clean_card_data(self, df):
        """
        Clean the card data by handling NULL values, errors with dates,
        and removing rows with incorrect or invalid values in specific columns.

        Args:
            df (pandas.DataFrame): DataFrame containing card data.

        Returns:
            pandas.DataFrame: Cleaned DataFrame.
        """
        # Drop rows with NULL values
        df = df.dropna()

        # Remove rows with invalid dates in date_payment_confirmed column
        df.loc[:, 'date_payment_confirmed'] = pd.to_datetime(df['date_payment_confirmed'], format='%Y-%m-%d', errors='coerce')


        # Remove rows with invalid expiry_date
        df = df.dropna(subset=['expiry_date'])

        # Ensure card_number column contains only numeric values
        df = df[df['card_number'].str.isnumeric()]

        pd.options.mode.chained_assignment = None

        # Return the cleaned DataFrame
        return df
    
    def clean_store_data(self,df):

        # Step 1: Remove the "index" column
        df.drop(columns=['index'], inplace=True)

        # Step 2: Handle missing values
        # Replace placeholders with actual NaN values
        df.replace(['N/A', 'None'], pd.NA, inplace=True)

        # Step 3: Convert data types if needed
        # Convert columns to appropriate data types
        df['longitude'] = pd.to_numeric(df['longitude'], errors='coerce')
        df['staff_numbers'] = pd.to_numeric(df['staff_numbers'], errors='coerce')
        df['opening_date'] = pd.to_datetime(df['opening_date'], errors='coerce')


        return df
    
    def convert_product_weights(self, csv_file_path):
        try:
            # Read the CSV file into a DataFrame
            products_df = pd.read_csv(csv_file_path)
            
            # Clean up the weight column
            products_df['weight'] = products_df['weight'].apply(self.clean_weight)
            
            # Convert weights to kg
            products_df['weight'] = products_df['weight'].apply(self.convert_to_kg)
            
            return products_df
        except Exception as e:
            print("An error occurred:", e)

    def clean_weight(self, weight):
        # Remove non-numeric characters and convert to lowercase
        cleaned_weight = re.sub(r'[^\d\.]', '', str(weight)).lower()
        return cleaned_weight

    def convert_to_kg(self, weight):
        if not weight:  # Check if weight is empty
            return 0.0  # Return 0.0 for empty values

        # Convert ml to g using a 1:1 ratio
        if 'ml' in weight:
            weight_in_kg = float(weight.replace('ml', '')) / 1000  # Convert ml to kg
        elif 'g' in weight:
            weight_in_kg = float(weight.replace('g', ''))
        else:
            weight_in_kg = float(weight) / 1000  # Assume other units are in grams and convert to kg
        return weight_in_kg
    
    def clean_products_data(self, products_df):
        try:
            # Remove rows with missing or NaN values
            products_df.dropna(inplace=True)

            # Remove duplicates, if any
            products_df.drop_duplicates(inplace=True)

            # Remove any additional erroneous values based on specific criteria
            # For example, you can remove rows where certain columns have unexpected values

            return products_df
        except Exception as e:
            print("An error occurred:", e)

    def clean_orders_data(self,orders_filepath):

        orders_filepath = '/Users/User/MRDC/Multinational-Retail-Data-Centralisation/databases/orders_table.csv'

        # Read the CSV file into a DataFrame
        orders_df = pd.read_csv(orders_filepath)

        # Remove columns first_name, last_name and 1
        orders_df = orders_df.drop(columns=['first_name', 'last_name', '1'])

        
        return orders_df
    
    def clean_date_details(self,date_file_path): 

        date_file_path = '/Users/User/MRDC/Multinational-Retail-Data-Centralisation/databases/date_details_data.csv'
        

        try:

            # Read the CSV file into a DataFrame
            date_data = pd.read_csv(date_file_path)

            # Remove rows with missing or NaN values
            date_data.dropna(inplace=True)

            # Remove duplicates, if any
            date_data.drop_duplicates(inplace=True)


            return date_data
        except Exception as e:
            print("An error occurred:", e)



