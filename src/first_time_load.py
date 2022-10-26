import pandas as pd
import sqlite3
class class_Date_Dim_loading:
    def __init__(self):
        print("Creating Connection with sqlite db")
        self.conn = sqlite3.connect("Movie_report.db")
        self.cur = self.conn.cursor()
        self.sql = """Create table if not exists  \n
                          DateDim(Date  INTEGER, \n
                          Month INTEGER, \n
                          Quarter INTEGER, \n                          
                          Year INTEGER, \n
                          Mon INTEGER)"""
        self.cur.execute(self.sql)
    def return_yyyymm_format(self,date):
        mon = str(date.month)
        year = str(date.year)
        return  int(year + mon.rjust(2, '0'))

    def Load_date_dimension(self, start_date,end_date):
        df = pd.DataFrame({"Date": pd.date_range(start_date, end_date,freq='M')})
        df['Month'] = df['Date'].apply(self.return_yyyymm_format)
        df["Quarter"] = df.Date.dt.quarter
        df["Year"] = df.Date.dt.year
        df['Mon'] = df.Date.dt.month
        df.to_sql("DateDim", self.conn, if_exists="replace", index=False)

    def __del__(self):
        print("Closing  connection for sqlite db")
        self.conn.close()
class class_movie_Dim_loading:
    def __init__(self):
        print("Creating Connection with sqlite db")
        self.conn = sqlite3.connect("Movie_report.db")
        self.cur = self.conn.cursor()
        self.sql = """Create table if not exists  \n
                          MovieDIM(movieID  INTEGER, \n
                          Genre TEXT, \n
                          Title TEXT)"""
        self.cur.execute(self.sql)
    def Load_movie_dimension(self, df):
        df.to_sql("MovieDIM", self.conn, if_exists="replace", index=False)

    def __del__(self):
        print("Closing  connection for sqlite db")
        self.conn.close()
if __name__ == "__main__":
    print("Loading Dimension")
    class_loading  =class_Date_Dim_loading()
    class_loading.Load_date_dimension('4/1/2000','3/1/2003')
    df_mov =pd.read_csv("..//data//Movie_details.gzip",compression='gzip')
    movie_loading =class_movie_Dim_loading()
    movie_loading.Load_movie_dimension(df_mov)