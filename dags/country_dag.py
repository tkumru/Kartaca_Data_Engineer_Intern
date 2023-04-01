from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator

import requests

from _mysql import MySQL
from _logging import setup_logger

import json

def start_logger():
    logger = setup_logger("start")
    
    logger.debug("country DAG is executing...")
    
def fetch_data():
    url = "http://country.io/names.json"
    response = requests.get(url)
    json_data = response.json()
    return json.dumps(json_data)

def insert_data(json_data):
    _mysql = MySQL(host="mysql")
    
    _mysql.connect_database()
    _mysql.cursor_object()
    
    _mysql.query("""
         CREATE TABLE IF NOT EXISTs country (
             short_name VARCHAR(5),
             country VARCHAR(50)
         )
     """)
    
    dict_data = json.loads(json_data)
    for shortname, country in dict_data.items():
        _mysql.query(query="""
             INSERT INTO country (short_name, country)
             VALUES (%s, %s)
         """, data=(shortname, country))
     
    _mysql.close()
    
def finish_logger():
    logger = setup_logger("finish")
    
    logger.debug("country DAG is finished.")
     
     
     
default_args = {
    "owner": "TalhaNebiKumru",
    "depends_on_past": False,
    "start_date": datetime.strftime(datetime.now(), 
                                    '%y-%m-%d'),
    "retries": 1,
    "catchup": False,
    "retry_delay": timedelta(minutes=2)
}

with DAG('country', default_args=default_args,
         schedule_interval='0 10 * * *') as dag:
    _start_logger = PythonOperator(task_id="start_logger",
                                   python_callable=start_logger,
                                   dag=dag)
    _fetch_data = PythonOperator(task_id="fetch_data",
                                 python_callable=fetch_data,
                                 dag=dag)
    _insert_data = PythonOperator(task_id='insert_data',
                                 python_callable=insert_data,
                                 op_kwargs={'json_data': '{{ ti.xcom_pull(task_ids="fetch_data") }}'},
                                 dag=dag)
    _finish_logger = PythonOperator(task_id="finish_logger",
                                    python_callable=finish_logger,
                                    dag=dag)
    
    _start_logger >> _fetch_data >> _insert_data >> _finish_logger
