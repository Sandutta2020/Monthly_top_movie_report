import pandas as pd
from preparing_monthly_feed import creating_monthly_feed
from Data_receiving import Data_receiving
from Data_Staging import  data_staging
from Data_mart import  creating_data_mart
import os
from datetime import datetime, timedelta
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