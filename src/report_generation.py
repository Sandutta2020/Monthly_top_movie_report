import matplotlib.pyplot as plt
from db_connector import SqliteDB_Connect
from dateutil.relativedelta import relativedelta
import pandas as pd
class report_generation(SqliteDB_Connect):
    def __init__(self):
        super().__init__()
    def report_generation_function(self):
        df =super().select_sql("report_sql")
        return df
    def return_job_run_date(self):
        df =super().select_sql("select_job_date")
        start_date = max(df["RUNDATE"])
        return start_date

def report_generation_monthwise():
    print("Creating report ")
    report_class =report_generation()
    df= report_class.report_generation_function()
    start_date =report_class.return_job_run_date()
    end_date = start_date + relativedelta(months=1)
    file_name =start_date.strftime("%Y%m")
    df.to_csv("..//report//monthly_report"+file_name+".csv",index=False)
    print("Creating report Completed")
    end_date = str(end_date)[0:10]
    report_class.set_rundate(end_date)

if __name__ == "__main__":
    print("Creating report ")
    report_generation_monthwise()