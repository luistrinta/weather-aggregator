from etl.pipeline import Pipeline
import os
from dotenv import load_dotenv
import polars as pl


load_dotenv()

#run the pipeline

def main():
    try:
        pipeline_open_weather = Pipeline(api_key=os.getenv("OPEN_WEATHER"),api_name="OPEN_WEATHER",api_url="https://api.openweathermap.org/data/2.5/weather")               
            
        #load csv with cities locations      
        location_df=pl.read_csv("./worldcities.csv",separator=",",has_header=True,infer_schema_length=10000)
        
        #filter by country, here we are using Portugal(PRT) as an example
        location_df= location_df.filter((location_df["admin_name"]=="Porto") | (location_df["admin_name"]=="Braga"))


        #iterate through every location
        for location in location_df.iter_rows():
        
            try:        
                pipeline_open_weather.run_pipeline(api="OPEN_WEATHER",location=(float(location[2]),float(location[3])))
            except:
                print("Pipeline exception found: Stopping pipeline...")
                pipeline_open_weather.running=False
                pipeline_open_weather.kill_all_threads()
                
    except KeyboardInterrupt:
                print("KeyboardInterrupt: Stopping pipeline...")
                pipeline_open_weather.running=False
                pipeline_open_weather.kill_all_threads()

if __name__ =="__main__":
    main()