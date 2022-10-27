import sqlite3
import pandas as pd
class class_movie_review_staging:
    def __init__(self):
        print("Creating Connection with sqlite db")
        self.conn = sqlite3.connect("Movie_report.db")
        self.cur = self.conn.cursor()
        self.sql = """Create table if not exists  \n
                          Movie_review_Fact_details_temp(MovieID  INTEGER, \n
                          Rating real, \n
                          Review_month INTEGER)"""
        self.cur.execute(self.sql)
    def return_yyyymm_format(self,date):
        mon = str(date.month)
        year = str(date.year)
        return  int(year + mon.rjust(2, '0'))
    def loading_dsa(self):
        print("Loading fact details")
        self.selectsql = "select  * from Movie_review_raw"
        df = pd.read_sql_query(self.selectsql, self.conn, parse_dates=["Review_date"])
        df['Review_month'] =df['Review_date'].apply(self.return_yyyymm_format)
        df =df[['MovieID','Rating','Review_month']].copy()
        df.to_sql("Movie_review_Fact_details_temp", self.conn, if_exists="replace", index=False)
        self.sql= "Insert into Movie_review_Fact_details select * from Movie_review_Fact_details_temp"
        self.cur.execute(self.sql)
        self.conn.commit()

def data_staging():
    cls1 = class_movie_review_staging()
    cls1.loading_dsa()


if __name__ == "__main__":
    data_staging()
