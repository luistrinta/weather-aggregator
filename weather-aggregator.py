import requests 
from dotenv import load_dotenv
import os
load_dotenv()


def get_current_weather(api_name,location):
    if api_name == "TOMORROW_IO":
        api_key = os.getenv(api_name)
        tomorrow_io = requests.get(f'https://api.tomorrow.io/v4/weather/realtime?location={location}&apikey={api_key}')
        print(tomorrow_io.text)
    elif api_name == "OPEN_WEATHER":
        api_key = os.getenv(api_name)
        lat=0
        lon=0
        
        open_weather = requests.get(f'https://api.openweathermap.org/data/2.5/weather?q={location}&appid={api_key}')
        print(open_weather.text)

print("TOMORROW_API")
get_current_weather("TOMORROW_IO","Porto")
print("\n\n")
print("OPEN WEATHER")
get_current_weather("OPEN_WEATHER","Porto")