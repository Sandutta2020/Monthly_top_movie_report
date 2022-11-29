import pandas as pd
import os
import fnmatch
import shutil
from db_connector import SqliteDB_Connect
import collections


class class_movie_review_raw(SqliteDB_Connect):
    def __init__(self):
        super().__init__()

    def insert_review_data(self, df):
        df["Review_month"] = self.file_name
        super().delete_sql("delete_movie_raw")
        df.to_sql(
            "Movie_review_raw_temp",
            self.conn,
            if_exists="replace",
            index=False)
        super().insert_sql("insert_movie_raw")


pattern = "User_review*.csv"


def Data_receiving():
    movie_db = class_movie_review_raw()
    csv_file_flag = False
    for file in os.listdir(
        os.path.join(
            os.path.split(os.path.abspath(__file__))[0], "..", "DRA")
    ):
        if fnmatch.fnmatch(file, pattern):
            print(file)
            csv_file_flag = True
            df = pd.read_csv(
                os.path.join(
                    os.path.split(
                        os.path.abspath(__file__))[0], "..", "DRA", file
                )
            )
            print(df.head(1))
            if check_quality(df):
                movie_db.insert_review_data(df)
                shutil.move(
                    os.path.join(
                        os.path.split(
                            os.path.abspath(__file__))[0], "..", "DRA", file
                    ),
                    os.path.join(
                        os.path.split(os.path.abspath(__file__))[0],
                        "..",
                        "DRA",
                        "Archive",
                        file,
                    ),
                )
            else:
                raise Exception("Please Check the input columns")
    if not csv_file_flag:
        print("No CSV files Present")
        raise Exception("No CSV files Present in DRA folder")


def check_quality(df):
    if len(df) == 0:
        print("The CSV is empty or Unreadable")
        return False
    input_column_list = list(df.columns)
    if collections.Counter(input_column_list) != collections.Counter(
        ["UserID", "MovieID", "Rating", "Review_date"]
    ):
        print("Please check the input CSV column names")
        return False
    return True


if __name__ == "__main__":
    print("Calling Data Receiving function")
    Data_receiving()
