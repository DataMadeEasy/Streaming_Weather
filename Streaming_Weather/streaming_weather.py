'''
Created on Dec 1, 2018

@author: tlrausch33
'''
from core import  getWeatherData_Forecast,getWeatherData_Observations
#from core import getWeatherData2
import time


print time.strftime("%m/%d/%Y %H:%M:%S")+ ' Weather Process Starting'
    
    
getWeatherData_Forecast('92109','US')
print time.strftime("%m/%d/%Y %H:%M:%S")+ ' Forecast added for 92109'

getWeatherData_Forecast('08540','US')     
print time.strftime("%m/%d/%Y %H:%M:%S")+ ' Forecast added for 08540'

    
getWeatherData_Observations('92109','US')
print time.strftime("%m/%d/%Y %H:%M:%S")+ ' Observation added for 92109'

getWeatherData_Observations('08540','US')     
print time.strftime("%m/%d/%Y %H:%M:%S")+ ' Observation added for 08540'    

print time.strftime("%m/%d/%Y %H:%M:%S")+ ' Weather Process Complete'
