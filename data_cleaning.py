import pandas as pd
import psycopg2
import sqlalchemy

class DataCleaning:
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
    
    def upload_to_db(self, cleaned_df):
        """
        Upload cleaned data to the PostgreSQL database.
        """
        conn_str = 'postgresql://postgres:Harvey16@localhost:5432/sales_data'
        engine = sqlalchemy.create_engine(conn_str)
        
        # Upload cleaned data to the database with the table name dim_users
        cleaned_df.to_sql(name='dim_users', con=engine, if_exists='replace', index=False)






# Path to the CSV file containing user data
csv_file_path = '/Users/User/MRDC/Multinational-Retail-Data-Centralisation/legacy_store.csv'

# Read the CSV file into a DataFrame
df = pd.read_csv(csv_file_path)

# Create an instance of DataCleaning
cleaner = DataCleaning()

# Clean the user data
cleaned_df = cleaner.clean_user_data(df)

# Now cleaned_df contains the cleaned user data

# uploading the clean data to DB
cleaner.upload_to_db(cleaned_df)