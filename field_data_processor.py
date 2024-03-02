"""
Module :FieldDataProcessor
This Module gave function  including ingestion,column renaming,
    applying corrections,and weather station mapping
"""




import pandas as pd
from data_ingestion import create_db_engine, query_data, read_from_web_CSV
import logging

class FieldDataProcessor:
    """
    A class to process field  data including ingestion,column renaming,
    applying corrections,and weather station mapping.
    
    
    """

    def __init__(self, logging_level="INFO"): # When we instantiate this class, we can optionally specify what logs we want to see

        # Initialising class with attributes we need. Refer to the code above to understand how each attribute relates to the code
        """
        initialization an instance of class of FieldDataProcessor .

        Paramaters:
        logging_level(str):The logging level to use (default is "INFO").
    
        """
        self.db_path = 'sqlite:///Maji_Ndogo_farm_survey_small.db'
        self.sql_query = """SELECT *
            FROM geographic_features
            LEFT JOIN weather_features USING (Field_ID)
            LEFT JOIN soil_and_crop_features USING (Field_ID)
            LEFT JOIN farm_management_features USING (Field_ID)
            """
        self.columns_to_rename = {'Annual_yield': 'Crop_type', 'Crop_type': 'Annual_yield'}
        self.values_to_rename = {'cassaval': 'cassava', 'wheatn': 'wheat', 'teaa': 'tea'}
        self.weather_map_data = "https://raw.githubusercontent.com/Explore-AI/Public-Data/master/Maji_Ndogo/Weather_data_field_mapping.csv"
        
        self.initialize_logging(logging_level)

        # We create empty objects to store the DataFrame and engine in
        self.df = None
        self.engine = None
        
    # This method enables logging in the class.
    def initialize_logging(self, logging_level):
        """
        Sets up logging for this instance of FieldDataProcessor.
        """
        logger_name = __name__ + ".FieldDataProcessor"
        self.logger = logging.getLogger(logger_name)
        self.logger.propagate = False  # Prevents log messages from being propagated to the root logger

        # Set logging level
        if logging_level.upper() == "DEBUG":
            log_level = logging.DEBUG
        elif logging_level.upper() == "INFO":
            log_level = logging.INFO
        elif logging_level.upper() == "NONE":  # Option to disable logging
            self.logger.disabled = True
            return
        else:
            log_level = logging.INFO  # Default to INFO

        self.logger.setLevel(log_level)

        # Only add handler if not already added to avoid duplicate messages
        if not self.logger.handlers:
            ch = logging.StreamHandler()  # Create console handler
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            ch.setFormatter(formatter)
            self.logger.addHandler(ch)

        # Use self.logger.info(), self.logger.debug(), etc.


    # let's focus only on this part from now on
    def ingest_sql_data(self):
        """
        ingest data from  SQL database .

        Returns:

        Pandas,Dataframe :The ingested data
        
        
        """

        self.logger.info("Sucessfully loaded data.")
        self.df = query_data(self.engine, self.sql_query)
        self.engine = create_db_engine(self.db_path)
        return self.df

    def rename_columns(self):
        """
        renames columns in the DatFrame according to predefined mapping.

        Returns:

        Pandas.dataFrame:The DatFrame with renamed columns.

        
        """
        # Annual_yield and Crop_type must be swapped
        pass

    def apply_corrections(self):
        """
        applying correction in the DataFrame.

        Returns:
        Pandas.DataFrame:The DatafFrame.
        
        
        """
        # Correct the crop strings, Eg: 'cassaval' -> 'cassava'
        pass

    def weather_station_mapping(self):
        """
        Maps weather Station data to the main DataFrame.

        Returns:
        pandasDataFrame:DataFrame with weather station data mapped.
        """
        # Merge the weather station data to the main DataFrame
        pass

    def process(self):
        """
        Process the field data step by step.

        Returns:

        Pandas.DatFrme:The processed DataFrame.
        
        """

        # This process calls the correct methods, and applies the changes, step by step. THis is the method we will call, and it will call the other methods in order.
        pass