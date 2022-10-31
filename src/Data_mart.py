import sqlite3
import pandas as pd
from db_connector import SqliteDB_Connect
class class_data_mart(SqliteDB_Connect):
    def __init__(self):
        super().__init__()
    def loading_fact(self):
        super().insert_sql('truncate_sql')
        self.insert_sql= """insert into Movie_review_fact(MovieID,
                     Review_month,
                     Monthly_rating_avg,
                     Monthly_rating_cnt,
                     Overall_rating_avg,
                     Overall_rating_cnt
                       )
                    select 
                    MovieID ,
                    Review_month,
                    avg(avg) over(partition by MovieID,Review_month) Monthly_rating_avg,
                    sum(count) over(partition by MovieID,Review_month ) Monthly_rating_cnt,
                    avg(avg) over(partition by MovieID order by Review_month ) Overall_rating_avg,
                    sum(count) over(partition by MovieID order by Review_month) Overall_rating_cnt
                    from 
                    (select 
	                count(*) count,
	                Avg(Rating) avg,
	                MovieID,
	                Review_month	from 
	                Movie_review_Fact_details
	                group by MovieID,Review_month)"""
        super().insert_sql('movie_fact',self.insert_sql)

def creating_data_mart():
    print("Loading Data Mart Function")
    cls1 =class_data_mart()
    cls1.loading_fact()

if __name__ == "__main__":
    creating_data_mart()
