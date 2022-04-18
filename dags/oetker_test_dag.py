"""
Author: kamrul Hasan
Date: 18.04.2022
Email: hasan.alive@gmail.com
"""


"""
This pipeline will transform the data little bit using some aggregation by pulling the data from postgres and then generate a static html report. 
"""

import json
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from datetime import datetime, timedelta
import urllib3
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator

###############################################
# Parameters
###############################################

# spark_master = "spark://spark:7077"
config_file = "/usr/local/spark/resources/json/config.json"

###############################################
# config loader and callback func
###############################################

with open(config_file, 'r') as f:
    config = json.load(f)


def download_data(*op_args):
    http = urllib3.PoolManager()
    r = http.request('GET', op_args[0])
    with open('../spark/resources/data/mock_data.json', 'wb') as outf:
        outf.write(r.data)


###############################################
# DAG Definition
###############################################
now = datetime.now()
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(now.year, now.month, now.day),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1)
}

#define the dag
dag = DAG(
    dag_id="oetker-task",
    description="This dag does very simple etl with the help of docker, pyspark and postgres.",
    default_args=default_args,
    schedule_interval=timedelta(1)
)

start = DummyOperator(task_id="start", dag=dag)

# simple python operator to download the data
get_data = PythonOperator(
    task_id='get_data',
    python_callable=download_data, op_args=[config['url']],
    dag=dag)

# create a temp table to persist the data from spark
create_table = PostgresOperator(
    task_id='create_persist_table',
    postgres_conn_id='postgres_default',
    sql="sql/create_table.sql",
    params=dict(table_name='oetker'),
    dag=dag

)

# running the spark job with spark submit operator
spark_job_extract = SparkSubmitOperator(
    task_id="spark_transform_job",
    application="/usr/local/spark/src/app/extract_app.py",
    name="extract_app",
    conn_id="spark_default",
    verbose=1,
    conf={"spark.master": config['spark_master']},
    py_files='/usr/local/spark/src/app/extract_job.py',
    application_args=[config_file],
    jars=config['postgres_driver_jar'],
    driver_class_path=config['postgres_driver_jar'],
    dag=dag)

# dummyoperator to indicate that the data will be loaded in enxt job. Ideally this task should be sparkjdbcoperator
# to load the dat but due to the usage old airflow 1.10 version this operator is not available
load_data = DummyOperator(task_id="load_data", dag=dag)

# running the spark job with spark submit operator
get_insight = SparkSubmitOperator(
    task_id="spark_insight_job",
    application="/usr/local/spark/src/app/insight_app.py",
    name="extract_app",
    conn_id="spark_default",
    verbose=1,
    py_files='/usr/local/spark/src/app/insight_job.py',
    conf={"spark.master": config['spark_master']},
    application_args=[config_file],
    jars=config['postgres_driver_jar'],
    driver_class_path=config['postgres_driver_jar'],
    dag=dag)

end = DummyOperator(task_id="end", dag=dag)

# create the dependency chain
start >> get_data >> create_table >> spark_job_extract >> load_data >> get_insight >> end
