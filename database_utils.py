from sqlalchemy import create_engine
import psycopg2
import yaml

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
        


connector = DatabaseConnector()

read_init_db = connector.init_db_engine('/Users/User/MRDC/Multinational-Retail-Data-Centralisation/db_creds.yaml')

print(read_init_db)