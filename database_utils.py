from sqlalchemy import create_engine

class DatabaseConnector:
    def __init__(self, db_uri):
        self.db_uri = db_uri
    
    def get_engine(self):
        engine = create_engine(self.db_uri)
        return engine
