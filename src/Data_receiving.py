import pandas as pd
import os
import fnmatch
import shutil
from db_connector import SqliteDB_Connect


class class_movie_review_raw(SqliteDB_Connect):
    def __init__(self):
        super().__init__()
    def insert_review_data(self, df):
        df.to_sql("Movie_review_raw_temp", self.conn, if_exists="replace", index=False)
        super().insert_sql('insert_movie_raw')

pattern = "User_review*.csv"
def Data_receiving():
    movie_db = class_movie_review_raw()
    csv_file_flag = False
    for file in os.listdir("..//DRA//"):
        if fnmatch.fnmatch(file, pattern):
            print(file)
            csv_file_flag = True
            df = pd.read_csv("..//DRA//" + file)
            print(df.shape)
            movie_db.insert_review_data(df)
            shutil.move("..//DRA//" + file, "..//DRA//Archive//" + file)
    if not csv_file_flag:
        print("No CSV files Present")
        raise Exception("No CSV files Present in DRA folder")


if __name__ == "__main__":
    print("Calling Data Receiving function")
    Data_receiving()
