import matplotlib.pyplot as plt
from db_connector import SqliteDB_Connect
from dateutil.relativedelta import relativedelta
import pandas as pd
class report_generation(SqliteDB_Connect):
    def __init__(self):
        super().__init__()
    def report_generation_function(self,Job_month,sql_name):
        df =super().select_sql(sql_name,{"jobmonth":Job_month})
        return df
    def return_job_run_date(self):
        df =super().select_sql("select_job_date")
        start_date = max(df["RUNDATE"])
        return start_date

def report_generation_monthwise():
    print("Creating report ")
    report_class =report_generation()
    start_date =report_class.return_job_run_date()
    end_date = start_date + relativedelta(months=1)
    file_name =start_date.strftime("%Y%m")
    df= report_class.report_generation_function(file_name,"report_sql")
    df.to_csv("..//report//monthly_report_"+file_name+".csv",index=False)
    print("Creating report Completed")
    end_date = str(end_date)[0:10]
    df_details =report_class.report_generation_function(file_name,"report_sql_pdf")
    last_6_months_df=report_class.report_generation_function(file_name,"last_6_months")
    #print(last_6_months_df.head())
    df_details_latest =df_details[df_details['Review_month'] ==file_name].copy()
    df_details_latest.sort_values(by=['rnk'],inplace=True)
    HTML_STR ="""<HTML><HEAD>
            <style>
            th {background-color: powderblue;align :center}
            td {text-align :center;font-size: 15px}
            table {border:1px solid black}
            H1   {color: blue;}
            p    {color: green;}
            </style>
            </HEAD><BODY>
            <H1> <center> Top 10 Movies Monthly Report </center> </H1>"""
    HTML_STR = HTML_STR +"<p>The Movie Review is showing for {}</p>".format(file_name)
    HTML_STR = HTML_STR+"""<TABLE><TR><TH> Movie Name</TH>
                            <TH colspan=3> RANK</TH>
                            <TH>Consequtive week</TH>
                            <TH colspan=3> Monthly Average</TH>
                            <TH colspan=3> Overall Average</TH>
                            <TH> Overall Viewers</TH></TR>"""
    print(df_details.head())
    for index,row in df_details_latest.iterrows():
        print(row['Title'])
        Movieid =row['MovieID']
        Monthly_count_diff =0
        Monthly_avg_diff =0
        overall_avg_diff =0
        rank_diff =0
        cnt =0
        for index_m,row_m in last_6_months_df.iterrows():           
            df_details_temp =df_details[(df_details['Review_month'] ==str(row_m['Month'])) & (df_details['MovieID'] ==Movieid)].copy()
            if len(df_details_temp) > 0:
                cnt =cnt +1
                if index_m ==0:
                    rank_diff =  int(df_details_temp['rnk']) -int(row['rnk'])
                    Monthly_count_diff =int(row['Rating_monthly_count']) - int(df_details_temp['Rating_monthly_count'])
                    Monthly_avg_diff =float(row['Rating_monthly_avg']) - float(df_details_temp['Rating_monthly_avg'])
                    overall_avg_diff =float(row['Overall_rating_avg']) - float(df_details_temp['Overall_rating_avg'])
            else:
                break
        table_data ="<tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>".format(row['Title'],row['rnk'],getting_image(rank_diff),rank_diff,cnt,row['Rating_monthly_avg'],getting_image(Monthly_avg_diff),round(Monthly_avg_diff,3),row['Overall_rating_avg'],getting_image(overall_avg_diff),round(overall_avg_diff,3),row['OverallCount'])
        HTML_STR =HTML_STR + table_data
    HTML_STR =HTML_STR + "</TABLE></BODY></HTML>"
    #print(HTML_STR)
    #report_class.set_rundate(end_date)
def getting_image(val):
    if val ==0:
        return "<img src='https://img.icons8.com/office/16/null/new.png'/>"
    elif val > 0:
        return "<img src='https://img.icons8.com/office/16/null/thick-arrow-pointing-up.png'/>"
    else:
        return "<img src='https://img.icons8.com/office/16/null/thick-arrow-pointing-down.png'/>"
if __name__ == "__main__":
    print("Creating report ")
    report_generation_monthwise()