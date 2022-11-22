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
        self.data_config = read_yaml(os.path.join(os.path.split(os.path.abspath(__file__))[0],"..","Conf.yaml"), 'select_sql')
        df = pd.read_sql_query("SELECT name FROM  sqlite_master  WHERE  type ='table' and name ='JOB_RUN_DATE'", self.conn)
        if len(df) > 0:
            self.selectsql= self.data_config['select_job_date']
            df = pd.read_sql_query(self.selectsql, self.conn,parse_dates=["RUNDATE"])
            start_date =max(df["RUNDATE"])
            self.file_name =   start_date.strftime("%Y%m")
            self.report_name_title = start_date.strftime("%b-%y")
    def insert_sql(self,insert_movie_raw):
        self.insert_sql= self.data_config[insert_movie_raw]
        self.cur.execute(self.insert_sql)
        self.conn.commit()
    def delete_sql(self,insert_movie_raw):
        self.delete_sql= self.data_config[insert_movie_raw]
        self.cur.execute(self.delete_sql)
        self.conn.commit()
    def select_sql(self,Sql_input,paramss={}):        
        self.selectsql= self.data_config[Sql_input]
        df = pd.read_sql_query(self.selectsql, self.conn, parse_dates=["Review_date","RUNDATE"],params=paramss)
        return df
    def create_sql(self,Sql_input):
        self.insert_sql= self.data_config[Sql_input]
        self.cur.execute(self.insert_sql)
    def set_rundate(self, updated_run_date):
        print("setting updated rundate with :", updated_run_date)
        self.updatesql = (
            """update JOB_RUN_DATE set RUNDATE =""" + "'" + updated_run_date + "'"
        )
        self.cur.execute(self.updatesql)
        self.conn.commit()    
    def __del__(self):
        print("Closing  connection for sqlite db")
        self.conn.close()
class selecting_movie(SqliteDB_Connect):
    pass


if __name__ == "__main__":
    db =selecting_movie()
    df =db.select_sql('select_movie_review')
    print(df.shape)
