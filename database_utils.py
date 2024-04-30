from sqlalchemy import create_engine
import psycopg2
import yaml
import sqlalchemy

class DatabaseConnector:
    def init_db_engine(self, file_path):
        creds = self.read_db_creds(file_path)
        RDS_HOST = creds['RDS_HOST']
        RDS_PASSWORD = creds['RDS_PASSWORD']
        RDS_USER = creds['RDS_USER']
        RDS_DATABASE = creds['RDS_DATABASE']
        RDS_PORT = creds['RDS_PORT']
        engine = create_engine(f"postgresql://{RDS_USER}:{RDS_PASSWORD}@{RDS_HOST}:{RDS_PORT}/{RDS_DATABASE}")
        return engine.connect()
    
    def read_db_creds(self, file_path):
        with open(file_path, 'r') as file:
            creds = yaml.safe_load(file)
            return creds
        
    def upload_to_db(self, table_name):
        """
        Upload cleaned data to the PostgreSQL database.
        """
        try:
            # Define the connection string
            conn_str = 'postgresql://postgres:Harvey16@localhost:5432/sales_data'
            
            # Create a SQLAlchemy engine
            engine = sqlalchemy.create_engine(conn_str)
            
            # Upload cleaned data to the database with the table name specified for use
            table_name.to_sql(name=table_name, con=engine, if_exists='replace', index=False)

            print("Data uploaded to the database successfully.")
        except Exception as e:
            print("An error occurred during data upload:", e)



        


connector = DatabaseConnector()

read_init_db = connector.init_db_engine('/Users/User/MRDC/Multinational-Retail-Data-Centralisation/db_creds.yaml')

print(read_init_db)