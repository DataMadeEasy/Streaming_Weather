import credentials
import json, time, threading,urllib2, urllib, datetime, requests
import psycopg2
from datetime import date
#import pyowm
#from kafka import KafkaConsumer, KafkaProducer

# -*- coding: utf-8 -*-










def getWeatherData_Forecast(zipcode,country):
    
    
    #DB Information
    try:
        dbconnection = psycopg2.connect( host=credentials.AWSDbCredentials['hostname'], user=credentials.AWSDbCredentials['username'], password=credentials.AWSDbCredentials['password'], dbname=credentials.AWSDbCredentials['database'], connect_timeout=1 )
        print "Connected to DB"

    except:
        print "Unable to connect to db"
        print credentials.AWSDbCredentials['hostname'] + credentials.AWSDbCredentials['username']+ credentials.AWSDbCredentials['password'] + credentials.AWSDbCredentials['database']
 
    
    
    
    
    response = requests.get('http://api.openweathermap.org/data/2.5/forecast?zip=' + zipcode + ',us&units=Imperial&cnt=80&APPID='+credentials.WeatherToken)
    jsonForecast =response.json()
    
    #print response.text
    #print jsonForecast['list']
    
    for datapoint in jsonForecast["list"]:
        
        ts = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')  
        
        #get temp and humidity information
        mainObservation = datapoint['main']
        dblTempMin = mainObservation['temp_min']
        dblTempMax = mainObservation['temp_max']
        dblTemp = mainObservation['temp']
        dblHumidity = mainObservation['humidity']
        
        
        #get cloud cover information
        cloudObservation = datapoint['clouds']
        dblCloudCover = cloudObservation['all']
        
        
        #get weather observations
        for weatherObservation in datapoint['weather']:
            strWeather = weatherObservation['main']
            strWeatherDescription = weatherObservation['description']
            
            
        
        windObservation = datapoint['wind']
        dblWindSpeed = windObservation['speed']
        
        strForecastedDate =  datapoint['dt_txt']
        
        
        

        
        cursor = dbconnection.cursor()
        query =  "INSERT INTO forecast (zipcode,temp_avg, temp_max, temp_min, humidity, cloud_percentage, weather_observation, weather_description,wind_speed, forecasted_timestamp, forecast_timestamp) VALUES ('{}',{},{},{},{},{},'{}','{}',{},'{}','{}');"
        cursor.execute(query.format(zipcode, dblTemp,dblTempMax, dblTempMin, dblHumidity, dblCloudCover,strWeather, strWeatherDescription,dblWindSpeed, strForecastedDate,ts))
        dbconnection.commit()
        
        
    
    dbconnection.close()
        

def getWeatherData_Observations(zipcode,country):
    
    
    #DB Information
    try:
        dbconnection = psycopg2.connect( host=credentials.AWSDbCredentials['hostname'], user=credentials.AWSDbCredentials['username'], password=credentials.AWSDbCredentials['password'], dbname=credentials.AWSDbCredentials['database'], connect_timeout=1 )
        print "Connected to DB"

    except:
        print "Unable to connect to db"
        print credentials.AWSDbCredentials['hostname'] + credentials.AWSDbCredentials['username']+ credentials.AWSDbCredentials['password'] + credentials.AWSDbCredentials['database']
 
 
    
    response = requests.get('http://api.openweathermap.org/data/2.5/weather?zip=' + zipcode + ',us&units=Imperial&cnt=80&APPID='+credentials.WeatherToken)
    jsonObservation =response.json()
    
    #print response.text
    #print jsonForecast['list']
    
    
        
    ts = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')  
        
    #get temp and humidity information
    mainObservation = jsonObservation['main']
    dblTempMin = mainObservation['temp_min']
    dblTempMax = mainObservation['temp_max']
    dblTemp = mainObservation['temp']
    dblHumidity = mainObservation['humidity']
    
    
    #get cloud cover information
    cloudObservation = jsonObservation['clouds']
    dblCloudCover = cloudObservation['all']
    
    
    #get weather observations
    for weatherObservation in jsonObservation['weather']:
        strWeather = weatherObservation['main']
        strWeatherDescription = weatherObservation['description']
        
        
    
    windObservation = jsonObservation['wind']
    dblWindSpeed = windObservation['speed']
    

    
    
    

    
    cursor = dbconnection.cursor()
    query =  "INSERT INTO observations (zipcode,temp_avg, temp_max, temp_min, humidity, cloud_percentage, weather_observation, weather_description,wind_speed,  observation_timestamp) VALUES ('{}',{},{},{},{},{},'{}','{}',{},'{}');"
    cursor.execute(query.format(zipcode, dblTemp,dblTempMax, dblTempMin, dblHumidity, dblCloudCover,strWeather, strWeatherDescription,dblWindSpeed, ts))
    dbconnection.commit()
        
        
    
    dbconnection.close()
        
        
        
                



     
    