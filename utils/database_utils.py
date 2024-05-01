from sqlalchemy import create_engine
import psycopg2
import sqlalchemy
import yaml

class DatabaseConnector:

    def __init__(self,file_path):
        self.creds = self.read_db_creds(file_path)
        self.engine = self.init_db_engine()

        
    def init_db_engine(self):
        RDS_HOST = self.creds['RDS_HOST']
        RDS_PASSWORD = self.creds['RDS_PASSWORD']
        RDS_USER = self.creds['RDS_USER']
        RDS_DATABASE = self.creds['RDS_DATABASE']
        RDS_PORT = self.creds['RDS_PORT']
        engine = create_engine(f"postgresql://{RDS_USER}:{RDS_PASSWORD}@{RDS_HOST}:{RDS_PORT}/{RDS_DATABASE}")
        return engine.connect()
    
    def read_db_creds(self, file_path):
        with open(file_path, 'r') as file:
            creds = yaml.safe_load(file)
            return creds
        
    def upload_to_db(self, df,table_name):
        """
        Upload cleaned data to the PostgreSQL database.
        """
        try:
            # Load credentials from the YAML file
            with open('/Users/User/MRDC/Multinational-Retail-Data-Centralisation/utils/db_creds.yaml', 'r') as file:
                creds = yaml.safe_load(file)

            # Extract credentials
            db_host = creds['DB_HOST']
            db_port = creds['DB_PORT']
            db_name = creds['DB_NAME']
            db_user = creds['DB_USER']
            db_password = creds['DB_PASSWORD']

            # Define the connection string
            conn_str = f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}'
            
            # Create a SQLAlchemy engine
            engine = sqlalchemy.create_engine(conn_str)
            
            # Upload cleaned data to the database with the table name specified for use
            df.to_sql(name=table_name, con=engine, if_exists='replace', index=False)

            print("Data uploaded to the database successfully.")
        except Exception as e:
            print("An error occurred during data upload:", e)


        