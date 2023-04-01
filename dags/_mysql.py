import mysql.connector

from _logging import setup_logger

class MySQL:
    def __init__(self, host="localhost", user="kartaca",
                 password="kartaca", database="airflow"):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        
    def connect_database(self):
        logger = setup_logger("connection")
        
        logger.debug("connect_database function is executing...")
        
        self.connection = mysql.connector.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                database=self.database
            )
        
        logger.info(f"Connected to {self.database}.")
        
    def cursor_object(self):
        self.cursor = self.connection.cursor()
    
    def query(self, query, data=None):
        logger = setup_logger("query")
        
        logger.debug("query function is executing...")
        
        self.cursor.execute(query, data) if data else self.cursor.execute(query)
        self.connection.commit()
        
        logger.info(f"{self.database} was updated.")
        
    def close(self):
        logger = setup_logger("close")
        
        logger.debug("close function is executing...")
        
        self.cursor.close()
        self.connection.close()
        
        logger.info("MySQL connection was closed.")
        
if __name__ == '__main__':
    mysql_ = MySQL()
    
    mysql_.connect_database()
    mysql_.cursor_object()
    
    mysql_.close()
