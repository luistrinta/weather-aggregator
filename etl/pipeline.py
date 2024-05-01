import os
from datetime import datetime
import uuid
from etl.loader import Loader
from etl.extractor import Extractor
from dotenv import load_dotenv
import time 
import threading 


class Pipeline:
    """Responsible for executing the ETL pipeline as a whole, using the Extractor and Loader classes for  data extraction and loading it into a database accordingly.
     The Pipeline class itself will also be responsible for any transformations done to the data e.g. merging data from our weather extraction APIs into a single entry that will cover more information"""
    
    _threads =[]
    

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
        self.running = True
        


    def run_pipeline(self,api,location,delta_timestamp=600):
        """Execute the pipeline. Starts the pipeline, extracting weather data for a given location (latitude and longitude) as well as a given API"""
        
        def every(location):
            try: 
                while self.running:
                    
                    current_date = datetime.utcnow().replace(second=0, microsecond=0).strftime('%Y-%m-%dT%H:%M:%SZ')
                  
                    print(f"INFO: Extracting data for {self.api_name} for location {location}. Time is: {current_date}")    
                    
                    #Only two API's supported ATM
                    if api == "OPEN_WEATHER":
                        try:
                            data = self.extract_data(query_string="lat={lat}&lon={lon}&appid={api_key}",api_key=self.extractor.api_key,lat=location[0],lon=location[1])
                            result = self.prepare_for_loading(table_name="OPEN_WEATHER" ,payload=data,current_date=current_date)
                            print(result)
                            self.load_data(query=result[0],values=result[1])
                        except Exception as error:
                             if self.running:
                                print(f"Error when processing data for OPEN_WEATHER API: {error}")
                             break
                    
                    for _ in range(delta_timestamp // 30):  # Split the sleep into periods of 3 seconds
                    
                        if not self.running:
                                break
                        time.sleep(30)
            
            except Exception:
                return
        try:        
            t = threading.Thread(target=lambda: every(location=location))
            t.start()

            self._threads.append(t)        
        
        except Exception as error:
            if self.running:
                print(f"Inside run_pipeline(self,api,location,delta_timestamp=600) at Pipeline class, error: {error}")
            t.join()
        
    def kill_all_threads(self):
        print("\n Executing shutdown...\n")
        self.running = False
        for t in self._threads:
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
            if self.running:
                print(f"Error when loading data into {table_name}: {error}")
    
    def load_data(self,query,values):
        """load the payload into the database, using the Loader class"""
        self.loader.load_data(query=query,payload=values)
        


