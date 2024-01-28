from datetime import datetime
from airflow.operators.bash import BashOperator
from airflow import DAG

with DAG('extract_nsdq', 
         schedule = None, 
         start_date=datetime(2023, 1, 1),
         catchup = False)as dag:
    extract_task = BashOperator(
    task_id='extract_task',
    bash_command = 'wget -c https://data.nasdaq.com/api/v3/datasets/WIKI/AAPL.json -O /workspaces/hands-on-introduction-data-engineering-4395021/lab/AAPL_nsdq.json'
    )
