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
    #create spark session


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
    # daily_df = spark.read.csv('Daily_Country_Wise_Confirmed_Cases.csv', header=True)
    daily_df = pd.read_csv('Daily_Country_Wise_Confirmed_Cases.csv')
    # daily_shape = (len(daily_df.columns), daily_df.count())
    daily_shape = daily_df.shape
    if daily_shape == shape_xcom:
        return daily_shape
    elif daily_shape[0] == shape_xcom[1]:
        rows = daily_shape[0] - shape_xcom[0]
        daily_df = daily_df.iloc[-rows]
        daily_df.set_index('Country', inplace=True)
        daily_df = daily_df.transpose()
        daily_df.reset_index(inplace=True)
        daily_df = daily_df.rename(columns={'index': 'Date'})
        daily_df.to_csv(f"dags/Daily_CC_{date.today()}", header=True)
        return daily_df.shape
    else:
        daily_df.set_index('Country', inplace=True)
        daily_df = daily_df.transpose()
        daily_df.reset_index(inplace=True)
        daily_df = daily_df.rename(columns={'index': 'Date'})
        daily_df.to_csv(f"dags/Daily_CC_{date.today()}", header=True)
        return daily_df.shape

# @task
# def load():
#     """
#     load to bigqquery
#     """


#set dag attributes
@dag(
    schedule_interval='@daily',
    start_date=datetime.utcnow(),
    catchup=False,
)
#function that calls all of our tasks to create dag
def pandas_dag():
    read_task = read()
    api_task = api_pull()
    update_task = update(read())
    echo_to_file=BashOperator(
        task_id='echo_to_file',
        bash_command=f'echo {update(read())} > /opt/airflow/dags/shape.txt')
    read_task >> api_task >> update_task >> echo_to_file

pandas_dag = pandas_dag()