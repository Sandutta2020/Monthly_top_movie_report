import sqlite3
import pandas as pd
class class_data_mart:
    def __init__(self):
        print("Creating Connection with sqlite db")
        self.conn = sqlite3.connect("Movie_report.db")
        self.cur = self.conn.cursor()
        self.sql = """Create table if not exists  \n
                          Movie_review_fact(MovieID  INTEGER, \n
                          Review_month INTEGER,\n
                          Monthly_rating_avg REAL,\n
                          Monthly_rating_cnt INTEGER,\n
                          Overall_rating_avg REAL,\n
                          Overall_rating_cnt INTEGER)"""
        self.cur.execute(self.sql)
    def loading_fact(self):
        self.trncate_sql ="delete from Movie_review_fact"
        self.cur.execute(self.trncate_sql)
        self.insert_sql= """insert into Movie_review_fact(MovieID,
                     Review_month,
                     Monthly_rating_avg,
                     Monthly_rating_cnt,
                     Overall_rating_avg,
                     Overall_rating_cnt
                       )
                    select 
                    MovieID ,
                    Months,
                    avg(avg) over(partition by MovieID,Months) Monthly_rating_avg,
                    sum(count) over(partition by MovieID,Months ) Monthly_rating_cnt,
                    avg(avg) over(partition by MovieID order by Months ) Overall_rating_avg,
                    sum(count) over(partition by MovieID ) Overall_rating_cnt
                    from 
                    (select 
	                count(*) count,
	                Avg(Rating) avg,
	                MovieID,
	                Months	from 
	                Movie_review_Fact_details
	                group by MovieID,Months)"""
        self.cur.execute(self.insert_sql)
        self.conn.commit()
if __name__ == "__main__":
    print("Calling creating_monthly_feed function")
    cls1 =class_data_mart()
    cls1.loading_fact()