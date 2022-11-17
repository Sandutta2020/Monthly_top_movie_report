import matplotlib.pyplot as plt
from db_connector import SqliteDB_Connect
from dateutil.relativedelta import relativedelta
import pandas as pd


class report_generation(SqliteDB_Connect):
    def __init__(self):
        super().__init__()

    def report_generation_function(self, Job_month, sql_name):
        df = super().select_sql(sql_name, {"jobmonth": Job_month})
        return df

    def return_job_run_date(self):
        df = super().select_sql("select_job_date")
        start_date = max(df["RUNDATE"])
        return start_date


def report_generation_monthwise():
    print("Creating report ")
    report_class = report_generation()
    start_date = report_class.return_job_run_date()
    end_date = start_date + relativedelta(months=1)
    file_name = start_date.strftime("%Y%m")
    report_name_title = start_date.strftime("%b-%y")
    # ----Normal Report
    print("Creating HTML Started")
    end_date = str(end_date)[0:10]
    df_details = report_class.report_generation_function(file_name, "report_sql_pdf")
    last_6_months_df = report_class.report_generation_function(
        file_name, "last_6_months"
    )
    # print(last_6_months_df.head())
    df_details_latest = df_details[df_details["Review_month"] == file_name].copy()
    df_details_latest.sort_values(by=["rnk"], inplace=True)
    HTML_STR = """<HTML><HEAD>
            <style>
            th {background-color: powderblue;}
            table {border:1px solid black}
            H1   {color: blue;}
            p    {color: green;font-weight: bold}
            .MV_Title{text-align :left;font-size: 16px;font-weight: bold}
            .MV_data{text-align :center;font-size: 15px}
            </style>
            </HEAD><BODY>
            <center><H1>  Top 10 Movies Monthly Report  </H1></center>"""
    HTML_STR = HTML_STR + "<center><p>The Movie Review is showing for {}</p></center>".format(
        report_name_title
    )
    HTML_STR = (
        HTML_STR
        + """<center><TABLE><TR><TH align ='left' > Movie Name</TH>
                            <TH colspan=3> RANK</TH>
                            <TH>Consequtive week</TH>
                            <TH colspan=3> Monthly Average</TH>
                            <TH colspan=3> Overall Average</TH>
                            <TH colspan=3> Monthly Viewers</TH>
                            <TH> Overall Viewers</TH></TR>"""
    )
    #print(df_details.head())
    for index, row in df_details_latest.iterrows():
        # print(row['Title'])
        Movie_data ={}
        Movieid = row["MovieID"]
        Movie_data['Movie_data'] =row["Title"]
        Monthly_count_diff = None
        Monthly_avg_diff = None
        overall_avg_diff = None
        rank_diff = None
        cnt = 0
        for index_m, row_m in last_6_months_df.iterrows():
            df_details_temp = df_details[
                (df_details["Review_month"] == str(row_m["Month"]))
                & (df_details["MovieID"] == Movieid)
            ].copy()
            if len(df_details_temp) > 0:
                cnt = cnt + 1
                if index_m == 0:
                    rank_diff = int(row["rnk"]) -int(df_details_temp["rnk"]) 
                    Monthly_count_diff = int(row["Rating_monthly_count"]) - int(
                        df_details_temp["Rating_monthly_count"]
                    )
                    Monthly_avg_diff = float(row["Rating_monthly_avg"]) - float(
                        df_details_temp["Rating_monthly_avg"]
                    )
                    overall_avg_diff = float(row["Overall_rating_avg"]) - float(
                        df_details_temp["Overall_rating_avg"]
                    )
            else:
                break
        Movie_data['rnk'] =row["rnk"]
        Movie_data['rank_icon'] =getting_image(rank_diff)
        Movie_data['rank_diff'] ='' if rank_diff ==None else rank_diff
        Movie_data['Consequitive_week'] =cnt
        Movie_data['Rating_monthly_avg']=row["Rating_monthly_avg"]
        Movie_data['Monthly_avg_diff_icon']=getting_image(Monthly_avg_diff)
        Movie_data['Monthly_avg_diff']='' if overall_avg_diff==None else round(Monthly_avg_diff,3)
        Movie_data['Overall_rating_avg']=row["Overall_rating_avg"]
        Movie_data['overall_avg_diff_icon']=getting_image(overall_avg_diff)
        Movie_data['overall_avg_diff']='' if overall_avg_diff==None else round(overall_avg_diff,3)
        Movie_data['Rating_monthly_count']=row["Rating_monthly_count"]
        Movie_data['Monthly_count_diff_icon']=getting_image(Monthly_count_diff)
        Movie_data['Monthly_count_diff']='' if Monthly_count_diff ==None else Monthly_count_diff
        Movie_data['OverallCount']=row['OverallCount']
        table_data = '<TR>'
        for key,value in Movie_data.items():
            if key =='Movie_data':
                table_data = table_data + "<td class='MV_Title'>{}</td>".format(value)
            else:
                table_data = table_data + "<td class='MV_data'>{}</td>".format(value)      

        HTML_STR = HTML_STR + table_data + '</TR>'
    HTML_STR = HTML_STR + "</TABLE></center></BODY></HTML>"
    # print(HTML_STR)
    with open("..//report//monthly_html_report_" + file_name + ".html", "w") as f:
        f.write(HTML_STR)
    report_class.set_rundate(end_date)


def getting_image(val):
    if val ==None :
        return "<img src='..//static//new.png'/>"
    if val == 0:
        return "<img src='..//static//icons8-equals-24.png'/>"
    elif val > 0:
        return "<img src='..//static//thick-arrow-pointing-up.png'/>"
    elif val <0:
        return "<img src='..//static//thick-arrow-pointing-down.png'/>"
    else :
        return "<img src='..//static//new.png'/>"


if __name__ == "__main__":
    print("Creating report ")
    report_generation_monthwise()
