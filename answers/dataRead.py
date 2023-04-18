from datetime import datetime

import pandas as pd
import numpy as np
import glob
import re
import logging

# df=pd.read_csv("Z:\\MyGit\\code-challenge-template\\wx_data\\USC00110072.txt", delimiter="\t", header=None)
# print(df.head())
# df.columns=['date','maxTemp','minTemp','precipitation']
# print(df.head())

import sqlite3
import csv
# conn = sqlite3.connect('weather.db')
#
# c = conn.cursor()
# c.execute("select * from weather")
# with open('weather.csv', 'r') as f:
#    reader = csv.reader(f)
#    for row in reader:
#        c.execute("INSERT INTO weather (station, date, max_temp, min_temp, precipitation) VALUES (?, ?, ?, ?, ?)", row)
# conn.commit()
# conn.close()


#####################################################
##########Problem 1 - Data Modeling##################
#####################################################
def connect_to_db():
    conn = sqlite3.connect('database.db')
    return conn


def create_db_table():
    try:
        conn = connect_to_db()
        # conn.execute('''DROP TABLE weather''')
        logging.info("Creating table Weather")
        conn.execute('''
            CREATE TABLE IF NOT EXISTS weather(
                date DATE,
                maxTemp INT,
                minTemp INT,
                precipitation INT,
                stationID varchar NOT NULL
            );
        ''')

        conn.commit()
        logging.info("Weather table created successfully")
    except:
        logging.error("Table creation FAILED!!!")
    finally:
        conn.close()


#####################################################
##########Problem 2 - Data Ingestion ################
#####################################################

def get_target():
    conn = connect_to_db()
    try:
        df = pd.read_sql_query("SELECT * FROM weather", conn)
        print("get source")
        df.head()
        print(df.count())

    except Exception as e:
        logging.error("Unable to read the table",e)

    return df

def concat_target():
    try:
        target_df=get_target()
        for filepath in glob.iglob('Z:\\MyGit\\code-challenge-template\\wx_data\\*.txt'):
            source_df = get_dataframe(filepath)
            final_df = pd.concat([source_df,target_df]).drop_duplicates().reset_index(drop=True)
            write_database(final_df)
    except Exception as e:
        logging.error("Failed to concatenate the source to target", e)

def write_database(df):
    try:
        conn=connect_to_db()
        df.to_sql('weather', conn, if_exists='replace', index=False)
        conn.commit()
    except Exception as e:
        logging.info("Failed to write into target table", e)

def get_dataframe(table_path):
    """
    This method receives table path, reads as a pandas dataframe, uses regex to extract and add stationID from table_path and performs some data preparation
    :param table_path: path which contains data to be ingested
    :returns: Pandas dataframe that can be ingested into the database
    """
    logging.info("Extracting data from source-"+table_path)
    logging.info("Start time: ",datetime.now())

    ######## Read data from source into a Pandas Dataframe for processing #############
    df = pd.read_csv(table_path, delimiter="\t", header=None)
    print(df.head())
    df.columns=['date','maxTemp','minTemp','precipitation']
    match = re.findall(r'(\w*)(?=.txt)', table_path)
    # print(match[0])
    df['stationID']=match[0]
    print(df.head())

    #### droup duplicate records #####
    df.drop_duplicates(inplace=True)
    logging.info("End time: ",datetime.now())
    logging.info("Number of records that will be inserted into target table:",df.count())
    return df

if __name__=="__main__":
    create_db_table()
    # print(get_users())

    get_dataframe('Z:\\MyGit\\code-challenge-template\\wx_data\\USC00110072.txt')
    get_target()
