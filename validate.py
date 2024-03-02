import re
import numpy as np
import pandas as pd
from field_data_processor import FieldDataProcessor
from weather_data_processor import WeatherDataProcessor
import logging 

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# config_params =    # Paste in your previous dictionary data in here

field_processor = FieldDataProcessor(config_params)
field_processor.process()
field_df = field_processor.df

weather_processor = WeatherDataProcessor(config_params)
weather_processor.process()
weather_df = weather_processor.weather_df

# Rename 'Ave_temps' in field_df to 'Temperature' to match weather_df
field_df.rename(columns={'Ave_temps': 'Temperature'}, inplace=True)