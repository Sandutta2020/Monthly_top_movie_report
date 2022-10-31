import sqlite3
import pandas as pd
import yaml
from yaml import Loader
from pathlib import Path
from typing import Union
import os

def read_yaml(file: Union[str, Path], key: str = None) -> dict:
    with open(file, "r") as fp:
        params = yaml.load(fp, Loader)
    return params[key] if key else params


class SqliteDB_Connect:
    def __init__(self):
        print("Creating Connection with sqlite db")
        DB_PATH =os.path.join(os.path.split(os.path.abspath(__file__))[0],"..","sqlite_DB","Movie_report.db")
        self.conn = sqlite3.connect(DB_PATH)
        self.cur =self.conn.cursor()
        self.data_config = read_yaml("..//Conf.yaml", 'select_sql')
    def insert_sql(self,insert_movie_raw,direct_sql=None):
        if direct_sql:
            self.insert_sql =direct_sql
        else:
            self.insert_sql= self.data_config[insert_movie_raw]
        self.cur.execute(self.insert_sql)
        self.conn.commit()
    def select_sql(self,Sql_input):        
        self.selectsql= self.data_config[Sql_input]
        df = pd.read_sql_query(self.selectsql, self.conn, parse_dates=["Review_date"])
        return df
    def __del__(self):
        print("Closing  connection for sqlite db")
        self.conn.close()
class selecting_movie(SqliteDB_Connect):
    pass


if __name__ == "__main__":
    db =selecting_movie()
    df =db.select_sql('select_movie_review')
    print(df.shape)
