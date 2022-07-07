from kaggle.api.kaggle_api_extended import KaggleApi
from pyspark.sql import SparkSession
from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
import os
from updatescript import update

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
    spark = SparkSession.builder.getOrCreate()

@task
def read():
    """
    Function that identifies current directory, locates txt file created by previous task, and reads the content
    """
    path = os.path.abspath(__file__)
    dir_name = os.path.dirname(path)
    with open(f"{dir_name}/shape.txt", 'r') as txt:
        return txt.read

@task
def main(shape_xcom):
    """
    main.py, update table with new data, do transformations
    """
    update(shape_xcom)

@task
def load():
    """
    load to bigqquery
    """

#set dag attributes
@dag(
    schedule_interval='@daily',
    start_date=datetime.utcnow(),
    catchup=False,
)
#function that calls all of our tasks to create dag
def monkeypox_dag():
    read_task = read()
    update_task = main(read_task)

    echo_shape = BashOperator(
    task_id='echo_shape',
    bash_command='')
