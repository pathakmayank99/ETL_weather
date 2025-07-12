# Importing all the required libraries

from airflow import DAG
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task
import pandas as pd
import pendulum
import requests
import json

# Declaring variables 

# Latitude and longitude of the location for which weather data will be fetched

latitude = '52.5244'
longitude = '13.4105'

# Connection Ids

POSTGRES_CONN_ID = 'postgres_default'
API_CONN_ID = 'open_meteo_api'

default_args={ 'owner': 'airflow', 'start_date': pendulum.now()}

# Main DAG

with DAG(dag_id ='weather_etl_pipeline',
        default_args=default_args,
        schedule='@daily',
        catchup=False) as dags:
    

    @task
    def extract_weather_data():
        ''' Extract weather data from Open-Meteo API using Airflow Connection.'''

        # Use HTTP Hook to get connection details from Airflow connections

        http_hook = HttpHook(http_conn_id=API_CONN_ID,method='GET')
        
        # Build the endpoint for data

        endpoint = f'/v1/forecast?latitude={latitude}&longitude={latitude}&daily=weather_code,temperature_2m_max,temperature_2m_min,sunrise,sunset,uv_index_max,rain_sum,precipitation_sum,wind_speed_10m_max,daylight_duration&timezone=auto'

        response = http_hook.run(endpoint)

        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"Failed to fetch weather data: {response.status_code}")
        

    @task
    def transform_weather_data(data):
        ''' Transform the extracted data into a dictionary.'''
        weather_data = data['daily']
        transformed_data = {
            'latitude': latitude,
            'longitude':longitude,
            'date':  weather_data['time'],
            'weather_code': weather_data['weather_code'],
            'temperature_max':weather_data['temperature_2m_max'],
            'temperature_min':weather_data['temperature_2m_min'],
            'sunrise':weather_data['sunrise'],
            'sunset':weather_data['sunset'],
            'uv_index_max':weather_data['uv_index_max'],
            'rain_sum': weather_data['rain_sum'],
            'precipitation_sum': weather_data['precipitation_sum'],
            'wind_speed_max': weather_data['wind_speed_10m_max'],
            'daylight_duration': weather_data['daylight_duration']

        }
        return transformed_data



    @task
    def load_weather_data(transformed_data):
        "Inserting all the data to postgreSQL database"
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        engine = pg_hook.get_sqlalchemy_engine()

        conn = pg_hook.get_conn()
        #cursor = conn.cursor()

    # Convert the transformed_data dict to a DataFrame
        df = pd.DataFrame({
            'latitude': [transformed_data['latitude']] * len(transformed_data['date']),
            'longitude': [transformed_data['longitude']] * len(transformed_data['date']),
            'date': transformed_data['date'],
            'weather_code': transformed_data['weather_code'],
            'temperature_max': transformed_data['temperature_max'],
            'temperature_min': transformed_data['temperature_min'],
            'sunrise': transformed_data['sunrise'],
            'sunset': transformed_data['sunset'],
            'uv_index_max': transformed_data['uv_index_max'],
            'rain_sum': transformed_data['rain_sum'],
            'precipitation_sum': transformed_data['precipitation_sum'],
            'wind_speed_max': transformed_data['wind_speed_max'],
            'daylight_duration': transformed_data['daylight_duration'],
         })
        
        # Write DataFrame to Postgres
        df.to_sql('weather_data', engine, if_exists='replace', index=False)

        '''
        # Create table 
        cursor.execute(""" CREATE TABLE IF NOT EXISTS WEATHER_DATA 
                       (
                        latitude FLOAT,
                        longitude FLOAT,
                        date DATE,
                        weather_code INT,
                        temperature_max FLOAT,
                        temperature_min FLOAT,
                        sunrise TIMESTAMP,
                        sunset TIMESTAMP,
                        uv_index_max FLOAT,
                        rain_sum FLOAT,
                        precipitation_sum FLOAT,
                        wind_speed_max FLOAT,
                        daylight_duration FLOAT   
                       );  
                    """ )

        cursor.execute("""
        INSERT INTO weather_data (latitude, longitude, date, weather_code, temperature_max, temperature_min, sunrise, sunset, uv_index_max, rain_sum, precipitation_sum, wind_speed_max, daylight_duration
         VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) """, 
         (
            transformed_data['latitude'],
            transformed_data['longitude'],
            transformed_data['date'],
            transformed_data['weather_code'],
            transformed_data['temperature_max'],
            transformed_data['temperature_min'],
            transformed_data['sunrise'],
            transformed_data['sunset'],
            transformed_data['uv_index_max'],
            transformed_data['rain_sum'],
            transformed_data['precipitation_sum'],
            transformed_data['wind_speed_max'],
            transformed_data['daylight_duration']
        ))

        conn.commit()
        cursor.close()
        '''
# DAG workflow

    weather_data_json = extract_weather_data()
    transformed_data = transform_weather_data(weather_data_json)
    load_weather_data(transformed_data) 
    
