from kaggle.api.kaggle_api_extended import KaggleApi
from pyspark.sql import SparkSession
from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
import os
# from updatescript import update
import pandas as pd
from datetime import datetime, date
import re
from google.cloud import bigquery
import pandas as pd 
import pandas_gbq

#create our tasks
@task
def api_pull():
    """
    pulls the updated file from Kaggle
    """
    api = KaggleApi()
    api.authenticate()
    #download dataset 
    api.dataset_download_file('deepcontractor/monkeypox-dataset-daily-updated','Daily_Country_Wise_Confirmed_Cases.csv')


@task
def read():
    """
    Function that identifies current directory, locates txt file created by previous task, and reads the content
    """
    path = os.path.abspath(__file__)
    dir_name = os.path.dirname(path)
    with open(f"{dir_name}/shape.txt", 'r') as txt:
        shape = txt.read()
        end_shape = re.findall('[0-9]{2}', shape)
        return tuple(end_shape)

@task
def update(shape_xcom):
    """
    Main function to create df of new data
    """
    #create df from new CSV and determine sghape
    daily_df = pd.read_csv('Daily_Country_Wise_Confirmed_Cases.csv')
    daily_shape = daily_df.shape
    # if there is no new data, simply return the existing shape
    if daily_shape == shape_xcom:
        return daily_shape
    #if there are new rows but no new columns, grab the new rows. The intention here is ease of scalability, so rather than completely overwriting the table every time, the new rows can be appended to the existing tables
    elif daily_shape[0] == shape_xcom[1]:
        rows = daily_shape[0] - shape_xcom[0]
        daily_df = daily_df.iloc[-rows]
        daily_df.set_index('Country', inplace=True)
        daily_df = daily_df.transpose()
        daily_df.reset_index(inplace=True)
        daily_df = daily_df.rename(columns={'index': 'Date'})
        daily_df.to_csv(f"dags/Daily_CC_{date.today()}", header=True)
        return daily_df.shape
    #if there are new columns, overwrite the dataset and create new CSV
    else:
        daily_df.set_index('Country', inplace=True)
        daily_df = daily_df.transpose()
        daily_df.reset_index(inplace=True)
        daily_df = daily_df.rename(columns={'index': 'Date'})
        daily_df.to_csv(f"dags/Daily_CC_{date.today()}", header=True)
        return daily_df.shape

@task
def load():
    """
    load to bigqquery
    """
    daily_cases = pd.read_csv(f"dags/Daily_CC_{date.today()}")
    client = bigquery.Client()
    dataset_id = f"{client.project}.monkeypox"
    dataset = bigquery.Dataset(dataset_id)
    dataset.location = 'us'
    dataset = client.create_dataset(dataset, exists_ok = True, timeout =100)
    project_id = 'dearliza'
    table_id = 'monkeypox.monkeypox_data'

    pandas_gbq.to_gbq(daily_cases, table_id, project_id = project_id, if_exists = 'replace', api_method = 'load_csv')

#set dag attributes
@dag(
    schedule_interval='@daily',
    start_date=datetime.utcnow(),
    catchup=False,
)
#function that calls all of our tasks to create dag
def pandas_dag():
    api_task = api_pull()
    read_task = read()
    update_task = update(read_task)
    load_task = load()
    echo_shape=BashOperator(
        task_id='echo_shape',
        bash_command=f'echo {update_task} > /opt/airflow/dags/shape.txt')
    api_task >> read_task >> update_task >> load_task >> echo_shape

pandas_dag = pandas_dag()