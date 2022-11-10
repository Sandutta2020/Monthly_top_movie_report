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
    def weighted_avg_func(self,df):
        numerator = df['Rating1']*1 + df['Rating2']*2 + df['Rating3']*3 +  df['Rating4']*4 + df['Rating5']*5 
        denominator = df['Rating1'] + df['Rating2'] + df['Rating3'] +  df['Rating4'] + df['Rating5']
        weighted_avg =(numerator) / ( denominator)
        return round(weighted_avg,2)
    def loading_dsa(self):
        print("Loading fact details")
        df=super().select_sql('select_movie_review')
        df['Review_month'] =df['Review_date'].apply(self.return_yyyymm_format)
        df =df[['MovieID','Rating','Review_month']].copy()
        df.to_sql("Movie_review_Fact_details_temp", self.conn, if_exists="replace", index=False)
        super().insert_sql('insert_movie_details_fact')
        df_run_date =super().select_sql("select_job_date")
        JOB_DATE = max(df_run_date["RUNDATE"]).strftime("%Y%m")
        df_latest =df[df['Review_month'].astype(str)==JOB_DATE].copy()
        df_pivot =pd.pivot_table(df_latest, index='MovieID',columns='Rating', aggfunc='count')
        df_pivot.fillna(0,inplace=True)
        df_pivot.columns =["Rating1","Rating2","Rating3","Rating4","Rating5"]
        df_pivot['Rating_count']=df_pivot["Rating1"]+df_pivot["Rating2"]+df_pivot["Rating3"]+df_pivot["Rating4"]+df_pivot["Rating5"]
        df_pivot['Review_month'] =JOB_DATE
        df_pivot['monthly_avg'] =df_pivot.apply(lambda row: self.weighted_avg_func(row), axis=1)
        df_pivot.reset_index(inplace=True)
        df_pivot.to_sql("Movie_ratings_monthly_temp", self.conn, if_exists="replace", index=False)
        print(df_pivot.head(1),df_pivot.shape)
def data_staging():
    cls1 = class_movie_review_staging()
    cls1.loading_dsa()


if __name__ == "__main__":
    data_staging()
