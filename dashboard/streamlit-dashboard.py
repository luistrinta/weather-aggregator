import streamlit as st
from dotenv import load_dotenv
import os
import psycopg2
import pandas as pd
import pydeck as pdk
import numpy as np

#change page layout
st. set_page_config(layout="wide")

#load environment variables for database access
load_dotenv()


def connect_to_database():
    try:
        conn = psycopg2.connect(database=os.getenv("DATABASE"),
                                    host=os.getenv("HOST"),
                                    user="lmtrinta",
                                    password="15565516",
                                    port=os.getenv("PORT")
        )
        return conn
    except Exception as error:
        print(f"Error connecting to database: {error}")

def fetch_data(connection,selected_api):
    cursor = connection.cursor()
    cursor.execute(f"SELECT DISTINCT temperature,humidity, location_lat, location_lon FROM {selected_api.lower()} src WHERE src.date = (SELECT MAX(date) FROM {selected_api.lower()})")
    data=cursor.fetchall()
    
    connection.commit()
    cursor.close()
    return data
    
with st.container():
    col1, col2 = st.columns(2)
    location= ["Lisbon","Porto"]
    api = ["OPEN_WEATHER","WEATHER_API","FULL_WEATHER_DATA"]
    
    selected_api = st.selectbox(
            'Choose the API you wish to view',
            api,index=0)
        
conn = connect_to_database()

data = fetch_data(conn,selected_api)

new_data=[]
for elem in data:
    for i in range(int(elem[0])):
       new_data.append(elem)

chart_data = pd.DataFrame(new_data,columns=["temperature","humidity","lat","lon"])

chart_data_temp = pd.DataFrame({"t":chart_data["temperature"],"lon":chart_data["lon"],"lat":chart_data["lat"]})

chart_data_humi = pd.DataFrame({"h":chart_data["humidity"],"lon":chart_data["lon"],"lat":chart_data["lat"]})
print(chart_data_humi)
print(chart_data_temp)


st.pydeck_chart(pdk.Deck(
    map_style=None,
    initial_view_state=pdk.ViewState(
        latitude=40.76,
        longitude=-8.1,
        zoom=11,
        pitch=50,
    ),
    layers=[
        pdk.Layer(
           'HexagonLayer',
           data=chart_data_temp,
           get_position='[lon, lat]',
           radius=8000,
           elevation_scale=7,
           elevation_range=[0, 1500],
           pickable=True,
           extruded=True,
        ),
        pdk.Layer(
            'ScatterplotLayer',
            data=chart_data_humi,
            get_position='[lon, lat]',
            get_color='[200, 30, 0, 160]',
            get_radius=200,
        ),
    ],
))



