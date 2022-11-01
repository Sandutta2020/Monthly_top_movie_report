import sqlite3
import pandas as pd
from db_connector import SqliteDB_Connect
class class_data_mart(SqliteDB_Connect):
    def __init__(self):
        super().__init__()
    def loading_fact(self):
        super().insert_sql('truncate_sql')
        super().insert_sql('insert_mart_sql')

def creating_data_mart():
    print("Loading Data Mart Function")
    cls1 =class_data_mart()
    cls1.loading_fact()

if __name__ == "__main__":
    creating_data_mart()
