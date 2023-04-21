from datetime import datetime
import pandas as pd
import numpy as np
import glob
import re
import logging
from flask import Flask, request, jsonify
logging.getLogger().setLevel(logging.INFO)
import sqlite3


#####################################################
##########Problem 1 - Data Modeling #################
#####################################################
def connect_to_db():
    """
    Connects to SQLite Database which can be referred for future use
    :return: conn => a connection to SQLite Database
    """
    conn = sqlite3.connect('database.db')
    return conn


def create_db_table():
    """
    Creates two Database Tables naming 'weather' and 'analytics'
    weather: table which consists of all the weather station data from Nebraska, Iowa, Illinois, Indiana, or Ohio.
    analytics: table which has year and Station wise column averages
    """
    try:
        conn = connect_to_db()
        # conn.execute('''DROP TABLE analytics''')
        logging.info("Creating table Weather")
        conn.execute('''
            CREATE TABLE IF NOT EXISTS weather(
                date DATE,
                maxTemp INT,
                minTemp INT,
                precipitation INT,
                stationID varchar NOT NULL );
                ''')

        conn.commit()
        logging.info("Weather table created successfully")

        logging.info("Creating table Analytics")
        conn.execute('''
                    CREATE TABLE IF NOT EXISTS analytics(
                        year INT NOT NULL,
                        stationID varchar NOT NULL,
                        maxTemp DECIMAL,
                        minTemp INT,
                        precipitation INT);
                        ''')

        conn.commit()
        logging.info("Analytics table created successfully")
    except Exception as e:
        logging.error("Table creation FAILED!!!", e)
    finally:
        conn.close()


#####################################################
##########Problem 2 - Data Ingestion ################
#####################################################

def get_table(table_name):
    """
    :param table_name: This method receives table name to connect to SQLite
    :return: df: returns a pandas data frame
    """
    conn = connect_to_db()
    try:
        df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
        print("get Target")
        print(df.head())
        print(df.count())

    except Exception as e:
        logging.error(f"Unable to read table",e)

    return df

def get_dataframe(table_path):
    """
    This method receives table path, reads as a pandas dataframe, uses regex to extract and add stationID from table_path and performs some data preparation
    :param table_path: path which contains data to be ingested
    :returns: Pandas dataframe that can be ingested into the database
    """
    logging.info("Extracting data from source- "+table_path)
    logging.info("Start time: %s",datetime.now())

    ######## Read data from source into a Pandas Dataframe for processing #############
    df = pd.read_csv(table_path, delimiter="\t", header=None)
    df.columns=['date','maxTemp','minTemp','precipitation']
    match = re.findall(r'(\w*)(?=.txt)', table_path)
    df['stationID']=match[0]
    print("############################################################")
    print("Adding Source Data Frame- ",match[0])

    #### droup duplicate records #####
    df.drop_duplicates(inplace=True)

    ### replacing -9999(null records) with Null values
    df['maxTemp'] = df['maxTemp'].replace(-9999,None)
    df['minTemp'] = df['minTemp'].replace(-9999,None)
    df['precipitation'] = df['precipitation'].replace(-9999,None)
    ### Converting date field to approproate format ###
    df['date'] = pd.to_datetime(df['date'], format="%Y%m%d")
    df['date']=df['date'].dt.date

    logging.info("End time: %s",datetime.now())
    logging.info("Number of records that will be inserted into target table: %s",df.shape[0])
    return df, match[0]

def write_database(df, table_name):
    """
    Converts and writes pandas dataframe to SQLite
    :param df: Receives a pandas dataframe to write to SQLite table
    :param table_name: table name from SQLite
    """
    try:
        conn=connect_to_db()
        df.to_sql(table_name, conn, if_exists='replace', index=False)
        conn.commit()
    except Exception as e:
        logging.info("Failed to write table", e)

def concat_target():
    """
    Reads source(from wx_data dir) and target table(from SQLite) and concatenates the dataframe. This method also handles duplicate records not
    being written
    """
    try:
        for filepath in glob.iglob('Z:\\MyGit\\code-challenge-template\\wx_data\\*.txt'):
            source_df, stationID = get_dataframe(filepath)
            target_df = get_table('weather')
            ### Below condition checks if the source dataframe for StationID is already added to target or not
            if stationID in target_df['stationID'].unique():
                logging.info(f'The source table for {stationID} is already complete')
                continue
            else:
                final_df = pd.concat([source_df,target_df]).reset_index(drop=True)
                final_df.drop_duplicates(inplace=True)
                write_database(final_df, 'weather')
    except Exception as e:
        logging.error("Failed to concatenate the source to target", e)


#####################################################
########## Problem 3 - Data Analysis ################
#####################################################

def get_analytics():
    df=get_table('weather')
    df_2=df.copy()
    df_2['date']=pd.to_datetime(df_2['date'], format="%Y-%m-%d")
    df_2['year']=df_2['date'].dt.year
    df_2 = df_2.astype({'maxTemp': 'float64', 'minTemp': 'float64', 'precipitation': 'float64', 'stationID': 'string',
                        'year': 'int64'})
    df_2.drop(['date'], axis=1,inplace=True)

    write_df=df_2.groupby(['year','stationID'])[["maxTemp","minTemp","precipitation"]].mean().reset_index()
    logging.info("Writing data to table analytics")
    write_database(write_df,'analytics')
    logging.info("Writing to table analytics complete!!!")


#####################################################
########## Problem 4 - REST API #####################
#####################################################
def get_weather_station_year(date,station):
    """
    :param date: receives date from user
    :param station: receives stationID from suer
    :return: weather details from Weather table for the respective date and station
    """
    data = {}
    try:
        conn = connect_to_db()
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute("SELECT * FROM weather WHERE date = ? and stationID=?", (date, station))
        row = cur.fetchone()
        data["date"] = row["date"]
        data["maxTemp"] = row["maxTemp"]
        data["minTemp"] = row["minTemp"]
        data["precipitation"] = row["precipitation"]
        data["stationID"] = row["stationID"]
    except:
        data = {}

    return jsonify(data)

def get_weather_stats(year,station):
    """
    :param year: Receives year from user
    :param station: receives stationID from user
    :return: returns weather statistics(avergae) for respective year and stationID
    """
    data = {}
    try:
        conn = connect_to_db()
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute("SELECT * FROM analytics WHERE year = ? and stationID=?", (year, station))
        row = cur.fetchone()
        data["year"] = row["year"]
        data["stationID"] = row["stationID"]
        data["maxTemp"] = row["maxTemp"]
        data["minTemp"] = row["minTemp"]
        data["precipitation"] = row["precipitation"]

    except:
        data = {}

    return jsonify(data)


app = Flask(__name__)
@app.route('/api/weather', methods=['GET'])
def api_weather():
    return get_weather_station_year('1985-01-01', 'USC00339312')

@app.route('/api/weather/stats', methods=['GET'])
def get_stats():
    return get_weather_stats('1985', 'USC00110072')


if __name__=="__main__":
    create_db_table()
    concat_target()
    get_analytics()
    # get_table('weather')
    # get_table('analytics')
    app.run(debug=True)

    #####################################################
    #################### Deployment #####################
    #####################################################
    """
    Deployment in AWS:
    
    
    ETL:
    Since the data is relatively small a simple Lambda or a Glue job for data processing(extraction, transformation, Loading) should suffice
    If however, the data is huge then maybe an EMR running on EC2 would be a viable option
    
    Storage:
    Data can be stored as Hive tables in an S3 location. An IAM role for the user profile should be added to make sure all the policy requirements
    are met.
    
    API:
        API gateway can be used to integrate with ETL to get the required results
        
    Additional querying:
    AWS Athena can be used since the data is stored as HIVE tables.
    
    """