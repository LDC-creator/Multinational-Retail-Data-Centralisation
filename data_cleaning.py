import pandas as pd
from datetime import datetime

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
        date_columns = ['birthdate', 'registration_date']
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

# Example usage:
# Create an instance of DataCleaning
cleaner = DataCleaning()

# Assuming df is the DataFrame containing user data
# Clean the user data
cleaned_df = cleaner.clean_user_data(df)

# Now cleaned_df contains the cleaned user data
