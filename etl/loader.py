import psycopg2
from datetime import datetime
from threading import Lock

class LoaderMeta(type):
    "Definition of a thread-safe singleton class for our database Loader"

    #We will start by making the Loader class into a singleton
    _instances = {}

    _lock : Lock = Lock()


    def __call__(cls,*args,**kwargs):
        """Possible changes to the __init__ argument do not affect the returned instance"""

        with cls._lock:

            if cls not in cls._instances:
                instance = super().__call__(*args,**kwargs)
                cls._instances[cls] = instance
            return cls._instances[cls]
   
class Loader(metaclass= LoaderMeta):
    "Responsible for loading our information into the database. In this case we will use SQL statements to insert the data into the database, also wel will create a connection per Loader Class"

    def __init__(self,database="",host="",user="",password="",port=5432) -> None:
        self.database = database
        self.host = host
        self.user = user
        self.password = password
        self.port = port
        try:
            self.connection = psycopg2.connect(database=database,
                                 host=host,
                                 user=user,
                                 password=password,
                                 port=port
    )
        except:
            print("Error while connecting to database: Invalid values. Please check your .env file or internet connection")

    def set_values(self,database=None,host=None,user=None,password=None,port=None) -> None:
        if database is not None:
            self.database = database
        if host is not None:
            self.host = host
        if user is not None:
            self.user = user
        if password is not None:
            self.password = password
        if port is not None:
            self.port = port

    def load_data(self,payload,query):
        """Responsible for inserting a given payload into the corresponding table inside a PostgresSQL database"""
        try:
            with Lock():
                cursor = self.connection.cursor()
                cursor.execute(query,payload)
                cursor.close()
                self.connection.commit()
                
            
        except Exception as error:
            cursor.close()
            self.connection.commit()
            print(f"Error when loading data into {self.database}: {error}")
