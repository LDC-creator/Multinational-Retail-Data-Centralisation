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
    
    def upload_to_db(self, cleaned_products_data):
        """
        Upload cleaned data to the PostgreSQL database.
        """
    try:
        # Define the connection string
        conn_str = 'postgresql://postgres:Harvey16@localhost:5432/sales_data'
        
        # Create a SQLAlchemy engine
        engine = sqlalchemy.create_engine(conn_str)
        
        # Upload cleaned data to the database with the table name dim_products
        cleaned_products_data.to_sql(name='dim_products', con=engine, if_exists='replace', index=False)
        print("Data uploaded to the database successfully.")
    except Exception as e:
        print("An error occurred during data upload:", e)


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



cleaner = DataCleaning()
csv_file_path = '/Users/User/MRDC/Multinational-Retail-Data-Centralisation/aws_products.csv'
cleaned_products_df = cleaner.convert_product_weights(csv_file_path)

# Clean additional erroneous values from the DataFrame
cleaned_products_df = cleaner.clean_products_data(cleaned_products_df)

# Upload cleaned products data to the database
cleaner.upload_to_db(cleaned_products_df)

print(cleaned_products_df)





# # Path to the CSV file containing user data
# csv_file_path = '/Users/User/MRDC/Multinational-Retail-Data-Centralisation/legacy_store_details.csv'

# # Read the CSV file into a DataFrame
# df = pd.read_csv(csv_file_path)

# Create an instance of DataCleaning
# cleaner = DataCleaning()
# # Clean the user data
# cleaned_df = cleaner.clean_user_data(df)

# # Now cleaned_df contains the cleaned user data


# Load your card data into a DataFrame (replace this with your actual data)
# card_data = pd.read_csv('/Users/User/MRDC/Multinational-Retail-Data-Centralisation/dim_card_details.csv')

# # Clean the card data
# cleaned_card_data = cleaner.clean_card_data(card_data)

# # Now cleaned_card_data contains the cleaned card data





# # Path to the CSV file containing store data
# csv_file_path = '/Users/User/MRDC/Multinational-Retail-Data-Centralisation/dim_store_details.csv'

# # Read the CSV file into a DataFrame
# df = pd.read_csv(csv_file_path)

# # Call the clean_store_data function to clean the store data
# cleaned_store_data = cleaner.clean_store_data(df)

# # Now cleaned_store_data contains the cleaned store data

# print(cleaned_store_data)


# # # # uploading the clean data to DB
# cleaner.upload_to_db(cleaned_store_data)