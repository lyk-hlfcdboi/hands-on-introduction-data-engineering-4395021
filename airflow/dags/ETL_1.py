from datetime import datetime, date
import pandas as pd
import json
import requests
from datetime import datetime
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow import DAG
from airflow.models import Variable

with DAG(
    'ETL_1', 
    schedule = None, 
    start_date=datetime(2023, 1, 1),
    catchup = False)as dag:

############# Define the functions for ETL works 
    def transform_data():
        with open('/workspaces/hands-on-introduction-data-engineering-4395021/lab/FB_nsdq.json', 'r', encoding = 'utf-8') as f:
            data = json.load(f)
        name = data['dataset']['name']
        columns = data['dataset']['column_names']
        values = data['dataset']['data']

        df_ = pd.DataFrame(columns = columns, data = values)
        data_dict = {'columns': columns, 'values': values}

        # Calculate the monthly data
        df_['Date'] = pd.to_datetime(df_['Date'])
        trans_data = df_.groupby(pd.Grouper(key = 'Date', freq = 'M')).agg({'High': 'max', 'Low': 'min', 'Volume': 'mean'})\
                    .copy().reset_index(drop = False).rename(columns = {'High': 'Monthly Max', 
                                                                    'Low': 'Monthly Low', 
                                                                    'Volume': 'Mean Volume per Day'})
        trans_data['Date'] = trans_data['Date'].dt.to_period('M')
        trans_data = trans_data.sort_values('Date', ascending = False).reset_index(drop = True)
        
        # Save to .csv
        trans_data.to_csv('/workspaces/hands-on-introduction-data-engineering-4395021/lab/monthly_info.csv', 
                          index = False, header = False)

        # Generate column types string for the loading bash command
        column_types = []
        for column, dtype in trans_data.dtypes.items():
            dtype_str = str(dtype)
            if 'float' in dtype_str:
                dtype_str = 'FLOAT'
            elif 'period[M]' in dtype_str:
                dtype_str = 'DATETIME'
            column_types.append(f"{column.replace(' ', '_')} {dtype_str}")

        formatted_column_types = ', '.join(column_types)

        # Save the column types string to an Airflow variable
        Variable.set("column_types", formatted_column_types)

    # Construct the commands for creating the database and importing the data
    def construct_load_command(**kwargs):
        column_types = Variable.get("column_types")
        create_table_command = f"CREATE TABLE IF NOT EXISTS monthly_info ({column_types});"
        load_command = f".separator \",\"\n.import /workspaces/hands-on-introduction-data-engineering-4395021/lab/monthly_info.csv monthly_info"

        # Combine the commands
        full_command = f'echo -e "{create_table_command}\n{load_command}" | sqlite3 /workspaces/hands-on-introduction-data-engineering-4395021/lab/challenge/challenge-load-db.db'
        
        # Push the full command to XCom
        kwargs['ti'].xcom_push(key = 'load_command', value = full_command)

############## Define the DAG

    extract_task = BashOperator(
    task_id='extract_task',
    bash_command = 'wget -c https://data.nasdaq.com/api/v3/datasets/WIKI/FB.json -O /workspaces/hands-on-introduction-data-engineering-4395021/lab/FB_nsdq.json')
   
    transform_task = PythonOperator(
        task_id = 'transform_data', 
        python_callable = transform_data, 
        dag = dag)

    command_constructor = PythonOperator(
        task_id = 'construct_load_command',
        python_callable = construct_load_command,
        provide_context = True,
        dag = dag)
    
    load_sql = BashOperator(
        task_id = 'to_sqlite', 
        bash_command = "{{ ti.xcom_pull(task_ids = 'construct_load_command', key = 'load_command') }}", 
        dag = dag)
    
########### Construct the DAG
    extract_task >> transform_task >> command_constructor >> load_sql






