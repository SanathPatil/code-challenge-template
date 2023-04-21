import unittest
import sqlite3
import pandas as pd


class TestMain(unittest.TestCase):
    ##################### Unit Testing #######################
    def get_db(self,table_name):
        """
        Connects to SQLite Database which can be referred for future use
        :return: conn => a connection to SQLite Database
        """
        conn = sqlite3.connect('database.db')
        df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
        return df

    def test_duplicates_weather(self):
        df=self.get_db('weather')
        self.assertEqual(df[df.duplicated()].shape, (0, 5))

    def test_duplicates_weather_stats(self):
        df=self.get_db('analytics')
        self.assertEqual(df[df.duplicated()].shape, (0, 5))

if __name__ == '__main__':
    unittest.main()