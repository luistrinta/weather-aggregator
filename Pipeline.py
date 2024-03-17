import requests
import os
import json
from string import Formatter
from datetime import datetime
import uuid
from dotenv import load_dotenv
import time 
import threading 
import psycopg2

class Pipeline:
    """Responsible for executing the ETL pipeline as a whole, using the Extractor and Loader classes for  data extraction and loading it into a database accordingly.
     The Pipeline class itself will also be responsible for any transformations done to the data e.g. merging data from our weather extraction APIs into a single entry that will cover more information"""
    
    def __init__(self,api_key,api_url,api_name="default") -> None:
        load_dotenv()
        self.pipeline_id = uuid.uuid4()
        self.loader = Loader(database=os.getenv("DATABASE"),
                             host=os.getenv("HOST"),
                             password=os.getenv("PASSWORD"),
                             port=os.getenv("PORT"),
                             user=os.getenv("USER")
                             )
        self.extractor = Extractor(api_key=api_key,
                                   api_url=api_url,
                                   api_name=api_name
                                  )
        self.api_name = api_name
   
    def run_pipeline(self,api,location,delta_timestamp=60):
        """Execute the pipeline. Starts the pipeline, extracting weather data for a given location (latitude and longitude) as well as a given API"""
        
        def every(location):
            try: 
                while True:
                    
                    current_date = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
                    print(f"INFO: Extracting data for {self.api_name} for location {location}. Time is: {current_date}")    
                    
                    if api == "OPEN_WEATHER":
                        data = self.extract_data(query_string="lat={lat}&lon={lon}&appid={api_key}",api_key=self.extractor.api_key,lat=location[0],lon=location[1])
                        result = self.prepare_for_loading(table_name="OPEN_WEATHER" ,payload=data,current_date=current_date)
                        self.load_data(query=result[0],values=result[1])
                    
                    elif api == "WEATHER_API":
                        data = self.extract_data(query_string="key={api_key}&q={lat},{lon}&aqi=no",api_key=self.extractor.api_key,lat=location[0],lon=location[1])
                        query,values = self.prepare_for_loading("WEATHER_API" ,data,current_date)
                        self.load_data(query=query,values=values)
                    
                    time.sleep(delta_timestamp)
            
            except KeyboardInterrupt:
                print("Client stopped")
        
        try:        
            t = threading.Thread(target=lambda: every(location=location))
            t.start()
                    
        except Exception as error:
            print(error)
            t.join()
        

    def extract_data(self,query_string,**kwargs):
        """Extracts data from the API using the Extractor class"""
        #print("Extracting data")
        return self.extractor.extract_data(query_string,**kwargs)
        
    
    def transform_data():
        """Transforms the given data based of a parameter type = MERGE or type=NORMALIZE, which will yeld different operations over the data"""
        pass
    
    def prepare_for_loading(self,table_name,payload,current_date):
        """Creates the SQL query, as well as prepares the data for the payload, that will be loaded by the Loader class"""
        try:
            if table_name == "WEATHER_API":
                
                # Safe from SQL injection
                query =f"INSERT INTO weather_api (date,location,location_lat,location_lon,temperature,apparent_temp,pressure,humidity,wind_dir,wind_gust,wind_speed,visibility,precipitation,weather_status,cloud_percentage,uv_value) VALUES (TO_TIMESTAMP(%s, 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"'),%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
                values = (current_date,
                        payload["location"]["name"],
                        payload["location"]["lat"],
                        payload["location"]["lon"],
                        payload["current"]["temp_c"],
                        payload["current"]["feelslike_c"],
                        payload["current"]["pressure_mb"],
                        payload["current"]["humidity"],
                        payload["current"]["wind_degree"],
                        payload["current"]["gust_kph"],
                        payload["current"]["wind_kph"],
                        payload["current"]["vis_km"],
                        payload["current"]["precip_mm"],
                        payload["current"]["condition"]["text"],
                        payload["current"]["cloud"],
                        payload["current"]["uv"],
                        )
                            
                return query,values
            
            elif table_name == "OPEN_WEATHER":
                query =f"INSERT INTO open_weather (date,location,location_lat,location_lon,temperature,temperature_min,temperature_max,apparent_temp,pressure,humidity,wind_dir,wind_speed,visibility,weather_status,sunset_date,sunrise_date) VALUES (TO_TIMESTAMP(%s, 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"'),%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,TO_TIMESTAMP(%s), TO_TIMESTAMP(%s)) "
                values = (current_date,
                        payload["name"],
                        payload["coord"]["lat"],
                        payload["coord"]["lon"],
                        int(payload["main"]["temp"])-273.16,
                        payload["main"]["temp_min"],
                        payload["main"]["temp_max"],
                        payload["main"]["feels_like"],
                        payload["main"]["pressure"],
                        payload["main"]["humidity"],
                        payload["wind"]["deg"],
                        payload["wind"]["speed"],
                        payload["visibility"],
                        payload["weather"][0]["main"],
                        payload["sys"]["sunset"],
                        payload["sys"]["sunrise"],
                        )
                return query,values
        except Exception as error:
            print(f"Error when loading data into {table_name}: {error}")
    
    def load_data(self,query,values):
        """load the payload into the database, using the Loader class"""
        self.loader.load_data(query=query,payload=values)
        

class Extractor:
    "Represents the API connection and data extraction manager for the pipeline"

    def __init__(self,api_name="",api_url="",api_key="") -> None:
        """ Define the URL and key for the API. Other functions inside the class will deal with API calls."""    
        self.api_name= api_name
        self.api_url= api_url
        self.api_key = api_key

    def set_values(self,api_name=None,api_url=None,api_key=None):
        if api_name is not None:
            self.api_name= api_name
        if api_url is not None:
            self.api_url= api_url
        if api_key is not None:
            self.api_key = api_key

    def extract_data(self,query_string,**kwargs):
            """Call the given API and extract data from any source 
            query_string: The part in the url for our API calls after the ? character. This is the part of the URL responsible for filtering the request
            kwargs: A dictionary of all the fields used in the query_string variable.
            This function allows the Extractor class to make personalised calls to the api based on the necessities of the user
            """
            try:       
                request_url =Formatter().format(self.api_url+"?"+query_string,**kwargs)
                payload = requests.get(request_url)
                return json.loads(payload.text)
                
            except Exception as error:
                print(f"Error when extracting data from API {self.api_name}: {error}")
    
class Loader:
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

    def load_data(self,payload,query,timestamp=datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')):
        """Responsible for inserting a given payload into the corresponding table inside a PostgresSQL database"""
        try:
            cursor = self.connection.cursor()
            cursor.execute(query,payload)
            self.connection.commit()
            cursor.close()
            
        except Exception as error:
            self.connection.commit()
            cursor.close()
            print(f"Error when loading data into {self.database}: {error}")
