import datetime
from datetime import timedelta
import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

from src.connect import fetch_covid_state_data, hdfs_job, sqoop_job

default_args = {
    'owner': 'airflow',
    'start_date': datetime.datetime(2020, 2, 7),
    # 'end_date': datetime(2018, 12, 30),
    'depends_on_past': False,
    # 'email': ['airflow@example.com'],
    # 'email_on_failure': False,
    # 'email_on_retry': False,
    # If a task fails, retry it once after waiting
    # at least 5 minutes
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}
dag = DAG(dag_id='CAS',
          default_args=default_args,
          description="Covid Analysis Service",
          schedule_interval=timedelta(days=1),
          )
# t1, t2 and t3 are examples of tasks created by instantiating operators

t1 = PythonOperator(task_id="fetch_data", python_callable=fetch_covid_state_data, dag=dag)

t2 = PythonOperator(task_id="Dump_data_hdfs", python_callable=hdfs_job, dag=dag)

t3 = PythonOperator(task_id="Dump_to_mysql", python_callable=sqoop_job, dag=dag)

t1 >> t2 >> t3
