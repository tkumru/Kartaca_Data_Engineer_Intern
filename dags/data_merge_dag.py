from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator

import sqlalchemy as db
from sqlalchemy.types import VARCHAR
import pandas as pd

from _logging import setup_logger

def start_logger():
    logger = setup_logger("start")
    
    logger.debug("data_merge DAG is executing...")
    
def data_merge():
    engine = db.create_engine('mysql+mysqlconnector://kartaca:kartaca@mysql/airflow')
    
    country = pd.read_sql('country', engine)
    currency = pd.read_sql('currency', engine)
    merged = pd.DataFrame(columns=['short_name', 'country', 'currency'])
    
    country_tmp = country.copy()
    currency_tmp = currency.copy()
    
    country_schema = {
        "short_name": VARCHAR(5),
        "country": VARCHAR(50)
    }
    currency_schema = {
        "short_name": VARCHAR(5),
        "currency": VARCHAR(5)
    }
    merged_schema = {
        "short_name": VARCHAR(5),
        "country": VARCHAR(50),
        "currency": VARCHAR(5)
    }
    
    country = country.iloc[0: 0]
    currency = currency.iloc[0: 0]
    
    merged.to_sql('data_merge', engine, if_exists='replace',
                  index=False, dtype=merged_schema)
    country.to_sql('country', engine, if_exists='replace',
                   index=False, dtype=country_schema)
    currency.to_sql('currency', engine, if_exists='replace',
                    index=False, dtype=currency_schema)
    
    merged = pd.merge(country_tmp, currency_tmp, on='short_name')
    merged.to_sql('data_merge', engine, if_exists='replace',
                  index=False, dtype=merged_schema)
    
def finish_logger():
    logger = setup_logger("finish")
    
    logger.debug("data_merge DAG is finished.")
    
default_args = {
    "owner": "TalhaNebiKumru",
    "depends_on_past": False,
    "start_date": datetime.strftime(datetime.now(), 
                                    '%y-%m-%d'),
    "retries": 1,
    "catchup": False,
    "retry_delay": timedelta(minutes=2)
}

with DAG('data_merge', default_args=default_args,
         schedule_interval='10 10 * * *') as dag:
    _start_logger = PythonOperator(task_id='start_logger',
                                   python_callable=start_logger,
                                   dag=dag)
    _merge_data = PythonOperator(task_id='merge_data',
                                 python_callable=data_merge,
                                 dag=dag)
    _finish_logger = PythonOperator(task_id='finish_logger',
                                    python_callable=finish_logger,
                                    dag=dag)
    
    _start_logger >> _merge_data >> _finish_logger