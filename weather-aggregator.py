import requests 
from dotenv import load_dotenv
import os
import psycopg2
import json
import threading
import time
from datetime import datetime

load_dotenv()
conn = psycopg2.connect(database=os.getenv("DATABASE"),
                                 host=os.getenv("HOST"),
                                 user=os.getenv("USER"),
                                 password=os.getenv("PASSWORD"),
                                 port=os.getenv("PORT")
)

def extract_weather_data(api_name,location):
   
    if api_name == "TOMORROW_IO":
        api_key = os.getenv(api_name)
        payload = requests.get(f'https://api.tomorrow.io/v4/weather/realtime?location={location}&apikey={api_key}')
        return json.loads(payload.text)
    
    elif api_name == "OPEN_WEATHER":
        api_key = os.getenv(api_name)
        payload = requests.get(f'https://api.openweathermap.org/data/2.5/weather?q={location}&appid={api_key}')
        return json.loads(payload.text)

def load_to_database(payload,table_name,current_date):
    #We create a cursor for queryig our database
    cursor = conn.cursor()
    
    if table_name == "TOMORROW_IO":
        
        # Safe from SQL injection
        query =f"INSERT INTO tomorrow_io (date,location,temperature,apparent_temp,pressure,humidity,wind_dir,wind_gust,wind_speed,visibility,precipitation,weather_code,rain_intensity,uv_value) VALUES (TO_TIMESTAMP(%s, 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"'),%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
        values = (current_date,
                  payload["location"]["name"],
                  payload["data"]["values"]["temperature"],
                  payload["data"]["values"]["temperatureApparent"],
                  payload["data"]["values"]["pressureSurfaceLevel"],
                  payload["data"]["values"]["humidity"],
                  payload["data"]["values"]["windDirection"],
                  payload["data"]["values"]["windGust"],
                  payload["data"]["values"]["windSpeed"],
                  payload["data"]["values"]["visibility"],
                  payload["data"]["values"]["precipitationProbability"],
                  payload["data"]["values"]["weatherCode"],
                  payload["data"]["values"]["rainIntensity"],
                  payload["data"]["values"]["uvHealthConcern"],
                )
                    
        cursor.execute(query,values)
        conn.commit()
        
    elif table_name == "OPEN_WEATHER":
        query =f"INSERT INTO open_weather (date,location,temperature,temperature_min,temperature_max,apparent_temp,pressure,humidity,wind_dir,wind_gust,wind_speed,visibility,weather_status,sunset_date,sunrise_date) VALUES (TO_TIMESTAMP(%s, 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"'),%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,TO_TIMESTAMP(%s), TO_TIMESTAMP(%s)) "
        values = (current_date,
                  payload["name"],
                  payload["main"]["temp"],
                  payload["main"]["temp_min"],
                  payload["main"]["temp_max"],
                  payload["main"]["feels_like"],
                  payload["main"]["pressure"],
                  payload["main"]["humidity"],
                  payload["wind"]["deg"],
                  payload["wind"]["gust"],
                  payload["wind"]["speed"],
                  payload["visibility"],
                  payload["weather"][0]["main"],
                  payload["sys"]["sunset"],
                  payload["sys"]["sunrise"],
                )
        cursor.execute(query,values)
        conn.commit()
 
def transform_data(data_record1,data_record2):
    #merge both data entries into a new table 
    pass   

def every(waiting_time,extract,transform,load,api_name,location):
    while True:
           
        current_date = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
        print(f"INFO: Extracting data for {api_name} for location {location}. Time is: {current_date}")    
        
        data = extract(api_name,location)
        load(data,api_name,current_date)
        time.sleep(waiting_time)


def run_pipeline():
    thread_list =[]
    location_list=["Lisbon","Porto"]
    api_names = ["TOMORROW_IO","OPEN_WEATHER"]
    for location in location_list:
        for name in api_names:
          
            t = threading.Thread(target=lambda: every(30,extract_weather_data,transform_data,load_to_database,name,location))
            t.start()
            thread_list.append(t)

def main():
    run_pipeline()

main()