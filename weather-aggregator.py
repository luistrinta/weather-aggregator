import requests 
from dotenv import load_dotenv
import os
import psycopg2
import json
import threading
import time
from datetime import datetime
import signal
import sys
import pandas as pd

load_dotenv()

def stop_threads(thread_list):
    for t in thread_list:
        t.join()
    print("All threads shutdown gracefully...")
    sys.exit(0)
    
try:
    thread_list =[]  
    conn = psycopg2.connect(database=os.getenv("DATABASE"),
                                 host=os.getenv("HOST"),
                                 user=os.getenv("USER"),
                                 password=os.getenv("PASSWORD"),
                                 port=os.getenv("PORT")
    )
except Exception as error:
    print(f"Error connecting to database: {error}")

def extract_weather_data(api_name,location):
    try:
        if api_name == "OPEN_WEATHER":
            api_key = os.getenv(api_name)
            payload = requests.get(f'https://api.openweathermap.org/data/2.5/weather?lat={location[0]}&lon={location[1]}&appid={api_key}')
            return json.loads(payload.text)
        elif api_name == "WEATHER_API":
            api_key = os.getenv(api_name)
            payload = requests.get(f'http://api.weatherapi.com/v1/current.json?key={api_key}&q={location[0]},{location[1]}&aqi=no')
            return json.loads(payload.text)
    except Exception as error:
        print(f"Error when extracting data from API: {error}")

def load_to_database(payload,table_name,current_date):
    #We create a cursor for queryig our database
    try:
        cursor = conn.cursor()
    
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
                        
            cursor.execute(query,values)
            conn.commit()
            cursor.close()
        
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
            cursor.execute(query,values)
            conn.commit()
            cursor.close()
    except Exception as error:
        etl_state=False
        conn.commit()
        cursor.close()
        print(f"Error when loading data into {table_name}: {error}")
        
def transform_data(data_record1,data_record2):
    #merge both data entries into a new table 
    pass   

def run_pipeline():
    def every(waiting_time,extract,transform,load,api_name,location):
      try: 
        while True:
            
            current_date = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
            print(f"INFO: Extracting data for {api_name} for location {location}. Time is: {current_date}")    
            
            data = extract(api_name,location)
            load(data,api_name,current_date)
            time.sleep(waiting_time)
      except KeyboardInterrupt:
        print("IoT Hub client stopped")
      finally:
        # Gracefully shutdown the client
        stop_threads(thread_list)
    location_list=pd.read_csv("./worldcities.csv",header=0)
    location_list= location_list[location_list["iso3"]=="PRT"]
    print(location_list)
    for location in location_list.iterrows():
            print((float(location[1]["lat"]),float(location[1]["lng"])))         
    try:        
        location_list=pd.read_csv("./worldcities.csv",header=0)
        location_list= location_list[location_list["iso3"]=="PRT"]
        
        api_names = ["WEATHER_API"]
        for location in location_list.iterrows():
            for name in api_names:
                try:
                    t = threading.Thread(target=lambda: every(300,extract_weather_data,transform_data,load_to_database,name,(float(location[1]["lat"]),float(location[1]["lng"]))))
                    t.start()
                    thread_list.append(t)
                except Exception as error:
                    print(error)
                    for t in thread_list:
                        t.join()
    except KeyboardInterrupt:
        print("\nReceived Ctrl+C... Stopping threads.")
        for t in thread_list:
            t.join()  # Wait for all threads to finish
        print("Threads successfully stopped.")

def main():
    
    signal.signal(signal.SIGINT, lambda: stop_threads( thread_list))
    signal.signal(signal.SIGBREAK, lambda: stop_threads(thread_list))  
      
    try:
        run_pipeline()
    except KeyboardInterrupt:
        print("IoT Hub client stopped")
    finally:
        # Gracefully shutdown the client
        stop_threads(thread_list)

main()