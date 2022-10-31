import sqlite3
import pandas as pd
from db_connector import SqliteDB_Connect
class class_movie_review_staging(SqliteDB_Connect):
    def __init__(self):
        super().__init__()
    def return_yyyymm_format(self,date):
        mon = str(date.month)
        year = str(date.year)
        return  int(year + mon.rjust(2, '0'))
    def loading_dsa(self):
        print("Loading fact details")
        df=super().select_sql('select_movie_review')
        df['Review_month'] =df['Review_date'].apply(self.return_yyyymm_format)
        df =df[['MovieID','Rating','Review_month']].copy()
        df.to_sql("Movie_review_Fact_details_temp", self.conn, if_exists="replace", index=False)
        super().insert_sql('insert_movie_details_fact')
def data_staging():
    cls1 = class_movie_review_staging()
    cls1.loading_dsa()


if __name__ == "__main__":
    data_staging()
