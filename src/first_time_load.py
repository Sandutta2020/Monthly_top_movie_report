import pandas as pd
import os
from db_connector import SqliteDB_Connect
import datetime


class class_Dimention_loading(SqliteDB_Connect):
    def __init__(self):
        super().__init__()
        print("Creating Connection with sqlite db")
        self.sql = """Create table if not exists\n
                          DateDim(Date  INTEGER,\n
                          Month INTEGER,\n
                          Quarter INTEGER,\n
                          Year INTEGER,\n
                          Mon INTEGER)"""
        self.cur.execute(self.sql)
        self.sql = """Create table if not exists  \n
                          MovieDIM(movieID  INTEGER, \n
                          Genre TEXT, \n
                          Title TEXT)"""
        self.cur.execute(self.sql)

    def return_yyyymm_format(self, date):
        mon = str(date.month)
        year = str(date.year)
        return int(year + mon.rjust(2, "0"))

    def Load_date_dimension(self, start_date, end_date):
        df = pd.DataFrame(
            {
                "Date": pd.date_range(
                    start_date,
                    end_date,
                    freq="M"
                    )
                }
            )
        df["Month"] = df["Date"].apply(self.return_yyyymm_format)
        df["Quarter"] = df.Date.dt.quarter
        df["Year"] = df.Date.dt.year
        df["Mon"] = df.Date.dt.month
        df["Review_mon"] = df.Date.apply(
            lambda x: datetime.datetime.strftime(x, "%b-%y")
        )
        df.to_sql("DateDim", self.conn, if_exists="replace", index=False)

    def Load_movie_dimension_from_dataframe(self, df, table_name):
        df.to_sql(table_name, self.conn, if_exists="replace", index=False)


class class_movie_facts_loading(SqliteDB_Connect):
    def __init__(self):
        super().__init__()
        print("Creating Connection with sqlite db")
        self.sql = """Create table if not exists  \n
                          Movie_review_raw(User_ID  INTEGER, \n
                          MovieID INTEGER, \n
                          Rating INTEGER, \n
                          Review_date Text,
                          Review_month Text)"""
        self.cur.execute(self.sql)
        self.sql = """Create table if not exists  \n
                          Movie_review_raw_temp(User_ID  INTEGER, \n
                          MovieID INTEGER, \n
                          Rating INTEGER, \n
                          Review_date Text,
                          Review_month Text)"""
        self.cur.execute(self.sql)
        self.sql = """Create table if not exists  \n
                          Movie_ratings_monthly(MovieID INTEGER, \n
                          Rating_1_count INTEGER,\n
                          Rating_2_count INTEGER, \n
                          Rating_3_count INTEGER, \n
                          Rating_4_count INTEGER, \n
                          Rating_5_count INTEGER, \n
                          Rating_monthly_count INTEGER, \n
                          Review_month Text ,\n
                          Rating_monthly_avg real, \n
                          OverallCount INTEGER ,\n
                          Overall_rating_avg real \n
                          )"""
        self.cur.execute(self.sql)
        self.sql = """Create table if not exists  \n
                          Movie_ratings_monthly_01(MovieID INTEGER, \n
                          Rating_1_count INTEGER,\n
                          Rating_2_count INTEGER, \n
                          Rating_3_count INTEGER, \n
                          Rating_4_count INTEGER, \n
                          Rating_5_count INTEGER, \n
                          Rating_monthly_count INTEGER, \n
                          Review_month Text ,\n
                          Rating_monthly_avg real, \n
                          OverallCount INTEGER ,\n
                          Overall_rating_avg real \n
                          )"""
        self.cur.execute(self.sql)

        def __repr__(self):
            return "class load is Complete"


def first_time_script():
    if os.path.exists(
        os.path.join(
            os.path.split(os.path.abspath(__file__))[0],
            "..",
            "sqlite_DB",
            "Movie_report.db",
        )
    ):
        print("DB Path Existed,Ignoring Dimension Loading")
    else:
        print("Loading Dimension as There is not db found")
        class_loading = class_Dimention_loading()
        class_loading.Load_date_dimension("4/1/2000", "3/1/2003")
        df_mov = pd.read_csv(
            "..//data//Movie_details.gzip",
            compression="gzip"
            )
        class_loading.Load_movie_dimension_from_dataframe(df_mov, "MovieDIM")
        fact_loading = class_movie_facts_loading()
        print(fact_loading)


if __name__ == "__main__":
    print("Calling Data Receiving function")
    first_time_script()
