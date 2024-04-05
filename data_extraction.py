import yaml
from sqlalchemy import create_engine

class DataExtractor:
    def read_db_creds(self, file_path):
        """
        Read database credentials from a YAML file.

        Args:
            file_path (str): Path to the YAML file containing credentials.

        Returns:
            dict: A dictionary containing the database credentials.
        """
        try:
            with open(file_path, 'r') as file:
                creds = yaml.safe_load(file)
                return creds
        except FileNotFoundError:
            print(f"File '{file_path}' not found.")
            return {}
        except yaml.YAMLError as e:
            print(f"Error parsing YAML file: {e}")
            return {}


    def init_db_engine(self, file_path):
        creds = self.read_db_creds(file_path)
        RDS_HOST = creds['RDS_HOST']
        RDS_PASSWORD = creds['RDS_PASSWORD']
        RDS_USER = creds['RDS_USER']
        RDS_DATABASE = creds['RDS_DATABASE']
        RDS_PORT = creds['RDS_PORT']
        engine = create_engine(f"postgresql://{RDS_USER}:{RDS_PASSWORD}@{RDS_HOST}:{RDS_PORT}/{RDS_DATABASE}")
        return engine
    
    def main(self):
        file_path = "db_creds.yaml"
        engine = self.init_db_engine(file_path)
        print(engine)
if __name__ == "__main__":
    DataExtractor().main()

        








# # Example usage:
# file_path = "/Users/User/MRDC/Multinational-Retail-Data-Centralisation/db_creds.yaml"
# extractor = DataExtractor()
# creds = extractor.read_db_creds(file_path)
# print(creds)


