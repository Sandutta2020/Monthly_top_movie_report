import sqlite3
import pandas as pd
import yaml
from yaml import Loader
from pathlib import Path
from typing import Union

def read_yaml(file: Union[str, Path], key: str = None) -> dict:
    with open(file, "r") as fp:
        params = yaml.load(fp, Loader)
    return params[key] if key else params


class SqliteDB_class:
    def __init__(self):
        print("Creating Connection with sqlite db")
        self.conn = sqlite3.connect("Movie_report.db")
    def insert_sql(self):
        self.insert_sql= """"""
        self.cur.execute(self.insert_sql)
        self.conn.commit()
    def select_sql(self,Sql_input):
        data_config = read_yaml("..//Conf.yaml", 'select_sql')
        print(data_config)
        self.selectsql= data_config[Sql_input]
        df = pd.read_sql_query(self.selectsql, self.conn)
        return df
    def __del__(self):
        print("Closing  connection for sqlite db")
        self.conn.close()
class selecting_movie(SqliteDB_class):
    pass


if __name__ == "__main__":
    db =selecting_movie()
    df =db.select_sql('select_movie_review')
    print(df.head())
