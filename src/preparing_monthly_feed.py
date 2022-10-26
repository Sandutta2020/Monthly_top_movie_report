import sqlite3
import pandas as pd
from dateutil.relativedelta import relativedelta


class class_Job_loading:
    def __init__(self):
        print("Creating Connection with sqlite db")
        self.conn = sqlite3.connect("Movie_report.db")
        self.cur = self.conn.cursor()
        self.sql = "Create table if not exists JOB_RUN_DATE(RUNDATE  DATE)"
        self.cur.execute(self.sql)
        self.selectsql = "select  RUNDATE from JOB_RUN_DATE"
        df = pd.read_sql_query(self.selectsql, self.conn)
        if len(df) == 0:
            print("First Table creating db")
            self.InsertSql = """INSERT INTO JOB_RUN_DATE(RUNDATE) \n
              VALUES('2000-04-01')"""
            self.cur.execute(self.InsertSql)
            self.conn.commit()

    def get_rundate(self):
        print("Get the last rundate")
        self.selectsql = "select  RUNDATE from JOB_RUN_DATE"
        df = pd.read_sql_query(self.selectsql, self.conn, parse_dates=["RUNDATE"])
        # print(df)
        return df

    def set_rundate(self, updated_run_date):
        print("setting updated rundate with :", updated_run_date)
        print(type(updated_run_date))
        self.updatesql = (
            """update JOB_RUN_DATE set RUNDATE =""" + "'" + updated_run_date + "'"
        )
        self.cur.execute(self.updatesql)
        self.conn.commit()
        # print(df)

    def __del__(self):
        print("Closing  connection for sqlite db")
        self.conn.close()


def creating_monthly_feed():
    job_class = class_Job_loading()
    df = job_class.get_rundate()
    start_date = max(df["RUNDATE"])
    end_date = start_date + relativedelta(months=1)
    df_user_reviews = pd.read_csv(
        "..//data//User_ratings.gzip", compression="gzip", parse_dates=["Review_date"]
    )
    delta_records = df_user_reviews.loc[
        (df_user_reviews["Review_date"] > start_date)
        & (df_user_reviews["Review_date"] < end_date)
    ]
    file_name = start_date.strftime("%Y%m")
    delta_records.to_csv("..//DRA//User_review" + file_name + ".csv", index=False)
    end_date = str(end_date)[0:10]
    job_class.set_rundate(end_date)


if __name__ == "__main__":
    print("Calling creating_monthly_feed function")
    creating_monthly_feed()
