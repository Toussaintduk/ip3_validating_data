"""
Module: data ingestion
this Module  provides function to digest from varois sources like database and csv files.
"""
from sqlalchemy import create_engine, text
import logging
import pandas as pd
# Name our logger so we know that logs from this module come from the data_ingestion module
logger = logging.getLogger('data_ingestion')
# Set a basic logging message up that prints out a timestamp, the name of our logger, and the message
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
# db_path = 'sqlite:///Maji_Ndogo_farm_survey_small.db'

# sql_query = """
# SELECT *
# FROM geographic_features
# LEFT JOIN weather_features USING (Field_ID)
# LEFT JOIN soil_and_crop_features USING (Field_ID)
# LEFT JOIN farm_management_features USING (Field_ID)
# """

# weather_data_URL = "https://raw.githubusercontent.com/Explore-AI/Public-Data/master/Maji_Ndogo/Weather_station_data.csv"
# weather_mapping_data_URL = "https://raw.githubusercontent.com/Explore-AI/Public-Data/master/Maji_Ndogo/Weather_data_field_mapping.csv"

# ### START FUNCTION

def create_db_engine(db_path):

    """
    create the a SQlAlchemy engine object from given database path

    parameters:
    db_path(str):dabase path 
    
    Returns:

    engine:the SQLALchemy engine object.

    Raises:
    ImportError :if SQLALchemy is not installed.
    Exception:if an error occured while  creating the engine.
    """
    try:
        engine = create_engine(db_path)
        # Test connection
        with engine.connect() as conn:
            pass
        # test if the database engine was created successfully
        logger.info("Database engine created successfully.")
        return engine # Return the engine object if it all works well
    except ImportError: #If we get an ImportError, inform the user SQLAlchemy is not installed
        logger.error("SQLAlchemy is required to use this function. Please install it first.")
        raise e
    except Exception as e:# If we fail to create an engine inform the user
        logger.error(f"Failed to create database engine. Error: {e}")
        raise e
    
def query_data(engine, sql_query):
    """
    creates the DataFrame from engine object of SQLALchemy using sql_query 

    Paramaters:

    engine:engine objects created form  sqlite database 
    sql_query:query text to  join tables to make  DataFrame

    Returns:

    DataFrame:combined tables into DataFrame using sql_query


    Raises:
    msg:if the query returned  an empty DataFrame.
    info:if the query excuted sucessfully.
    ValueError: if SQL query Failed.
    Exception:if there is an error while querying the databse
    
    """
    try:
        with engine.connect() as connection:
            df = pd.read_sql_query(text(sql_query), connection)
        if df.empty:
            # Log a message or handle the empty DataFrame scenario as needed
            msg = "The query returned an empty DataFrame."
            logger.error(msg)
            raise ValueError(msg)
        logger.info("Query executed successfully.")
        return df
    except ValueError as e: 
        logger.error(f"SQL query failed. Error: {e}")
        raise e
    except Exception as e:
        logger.error(f"An error occurred while querying the database. Error: {e}")
        raise e
    
def read_from_web_CSV(URL):
    try:
        df = pd.read_csv(URL)
        logger.info("CSV file read successfully from the web.")
        return df
    except pd.errors.EmptyDataError as e:
        logger.error("The URL does not point to a valid CSV file. Please check the URL and try again.")
        raise e
    except Exception as e:
        logger.error(f"Failed to read CSV from the web. Error: {e}")
        raise e
    
### END FUNCTION