import pandas as pd
import os
import sqlite3
import fnmatch
import shutil


class class_movie_review_raw:
    def __init__(self):
        print("Creating Connection with sqlite db")
        self.conn = sqlite3.connect("Movie_report.db")
        self.cur = self.conn.cursor()
        self.sql = """Create table if not exists  \n
                          Movie_review_raw(User_ID  INTEGER, \n
                          MovieID INTEGER, \n
                          Rating INTEGER, \n
                          Review_DATE Text)"""
        self.cur.execute(self.sql)

    def insert_review_data(self, df):
        print("Get the last rundate")
        df.to_sql("Movie_review_raw", self.conn, if_exists="replace", index=False)

    def __del__(self):
        print("Closing  connection for sqlite db")
        self.conn.close()


pattern = "User_review*.csv"


def Data_receiving():
    movie_db = class_movie_review_raw()
    for file in os.listdir("..//DRA//"):
        if fnmatch.fnmatch(file, pattern):
            print(file)
            df = pd.read_csv("..//DRA//" + file)
            print(df.shape)
            movie_db.insert_review_data(df)
            shutil.move("..//DRA//" + file, "..//DRA//Archive//" + file)


if __name__ == "__main__":
    print("Calling creating_monthly_feed function")
    Data_receiving()
