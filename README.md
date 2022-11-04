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
to create a yaml from sql query:
```
import yaml
sql = ("""""")
app_config = dict(sql=sql)
print(yaml.dump(app_config))
```

1) Load Tables initially
python first_time_load.py

2) Create the following folder
	1)DRA under main folder and ceate a folder Archive Under DRA
	2) sqlite_DB
	3) report

load_examples = False

List of process running at 8080
```
lsof -i tcp:8080
```
Reference :https://betterdatascience.com/apache-airflow-install/
