```
python3 -m venv airflow_env
source airflow_env/bin/activate
```

pip install "apache-airflow==2.2.3" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.2.3/constraints-no-providers-3.9.txt"
```
cd airflow
airflow db init
airflow users create --username admin --password admin --firstname Santanu --lastname Dutta --role Admin --email santanukolkata@yahoo.in
```
```
airflow webserver -D
airflow scheduler -D
```
load_examples = False

Reference :https://betterdatascience.com/apache-airflow-install/
