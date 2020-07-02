from airflow import DAG
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.hive_operator import HiveOperator
from airflow.operators.mysql_operator import MySqlOperator
from airflow.contrib.operators.sqoop_operator import SqoopOperator
from datetime import datetime
import tasks
import os

default_args = {
    "start_date": datetime(2020, 6, 30),
    "owner": "airflow"
}
with DAG(dag_id="covid_dag", schedule_interval="@daily", default_args=default_args, catchup=False) as dag:
    get_data_fromAPI = PythonOperator(task_id='get_data_fromAPI', python_callable=tasks.validate_data)

    storing_file_to_HDFS = BashOperator(task_id='storing_file_to_HDFS',
                                        bash_command="hadoop fs -put -f /tmp/covidData_fetched.csv /sqoop_data/")

    # create_mysql_table = PythonOperator(task_id='create_mysql_table', python_callable=tasks.create_table)

    load_table = PythonOperator(task_id='load_table', python_callable=tasks.load_data)


    # load_into_mysql_table = BashOperator(task_id='load_into_mysql_table',
    #                                      bash_command="sqoop export --connect 'jdbc:mysql://localhost/covid' --username root --password Qwerty@123 --table corona --export-dir '/sqoop_data/'")

    # os.system(
    #     "sqoop export   --table {}  --connect jdbc:mysql://localhost:3306/{}   --username {}   --password "
    #     "{}   --export-dir /user/covid_data_{}.csv   --columns date,state,cases".format(table_name,
    #                                                                                     db,
    #                                                                                     username,
    #                                                                                     password, date))
    # create_hive_table = HiveOperator(task_id='create_hive_table',
    #                                  hql="create table covid.corona(active int,confirmed int,deaths int,recovered int,state string) row format delimited fields terminated by ',' stored as textfile;"
    #
    #                                  )
    # load_data = HiveOperator(task_id='load_data',
    #                          hql="LOAD DATA INPATH '/user/covidData_fetched.csv' INTO TABLE covid.corona")

    # get_data_fromAPI >> storing_file_to_HDFS >> create_mysql_table >> load_table

