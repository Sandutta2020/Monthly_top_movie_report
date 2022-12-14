import pandas as pd
from preparing_monthly_feed import creating_monthly_feed
from Data_receiving import Data_receiving
from Data_Staging import  data_staging
from Data_mart import  creating_data_mart
from report_generation import report_generation_monthwise
from first_time_load import first_time_script
import os
from datetime import datetime, timedelta
"""
from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable
with DAG(
    dag_id='MovieReviewLoading',
    schedule_interval=None,
    start_date=datetime(year=2022, month=10, day=1),
    catchup=False
) as dag:
    
    # 1. Get current datetime
    t1= PythonOperator(task_id='creating_monthly_feed', python_callable=creating_monthly_feed)
    t2= PythonOperator(task_id='Data_receiving', python_callable=Data_receiving)
    t3= PythonOperator(task_id='Data_Staging', python_callable=data_staging)
    t4= PythonOperator(task_id='creating_data_mart', python_callable=creating_data_mart)
    t1 >> t2>>t3>>t4
"""
if __name__ == "__main__":
    first_time_script()
    print("First_time Data Complete----------")
    creating_monthly_feed()
    print("Monthly Feed Complete----------")
    Data_receiving()
    print("Data Receiving Complete--------")
    data_staging()
    print("Loading Staging Complete-------")
    creating_data_mart()
    print("Loading Data Mart  Complete----")
    report_generation_monthwise()
    print("Report Generation Complete")
