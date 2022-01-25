
"""
Airflow Context & Templating
"""

# Instructions
# Use the Airflow context in the pythonoperator to complete the TODOs below. Once you are done, run your DAG and check the logs to see the context in use.

import datetime
import logging

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.S3_hook import S3Hook


def log_details(*args, **kwargs):
    #
    # NOTE: Look here for context variables passed in on kwargs:
    #       https://airflow.apache.org/docs/apache-airflow/stable/macros-ref.html
    #
    logging.info(f"Execution date is {ds}")
    logging.info(f"My run id is {run_id}")

dag = DAG(
    'lesson1.exercise5',
    schedule_interval="@daily",
    start_date=datetime.datetime.now() - datetime.timedelta(days=2)
)

list_task = PythonOperator(
    task_id="log_details",
    python_callable=log_details,
    provide_context=True,  # specify this option to have Airflow generate the automatic variables that we can reference above
    dag=dag
)

