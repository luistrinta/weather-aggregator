from Pipeline import Pipeline
import os
from dotenv import load_dotenv
import pandas as pd
import sys

load_dotenv()

#run the pipeline

def main():
    pipeline_open_weather = Pipeline(api_key=os.getenv("OPEN_WEATHER"),api_name="OPEN_WEATHER",api_url="https://api.openweathermap.org/data/2.5/weather")
    
    location_list=pd.read_csv("./worldcities.csv",header=0)
    location_list= location_list[location_list["iso3"]=="PRT"]
            
        
    #load csv with cities locations      
    location_list=pd.read_csv("./worldcities.csv",header=0)
        
    #filter by country, here we are using Portugal(PRT) as an example
    location_list= location_list[location_list["iso3"]=="PRT"]
        
    #iterate through every location
    for location in location_list.iterrows():
           
        try:        
            pipeline_open_weather.run_pipeline(api="OPEN_WEATHER",location=(float(location[1]["lat"]),float(location[1]["lng"])))
                    
        except Exception as error:
            print(error)
            sys.exit(0)

if __name__ =="__main__":
    main()