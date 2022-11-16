import sqlite3
import pandas as pd
from db_connector import SqliteDB_Connect
class class_data_mart(SqliteDB_Connect):
    def __init__(self):
        super().__init__()
    def loading_fact(self):
        super().insert_sql('truncate_sql')        
        super().insert_sql('insert_mart_sql')
        super().insert_sql('delete_temp_monthly_insert')
        super().delete_sql('delete_movie_rating_monthly')
        super().insert_sql('merge_sql')
        super().insert_sql('monthly_rating_insert')
        super().insert_sql('update_sql')
    def loading_agg_fact(self):
        pass


def creating_data_mart():
    print("Loading Data Mart Function")
    cls1 =class_data_mart()
    cls1.loading_fact()

if __name__ == "__main__":
    creating_data_mart()
