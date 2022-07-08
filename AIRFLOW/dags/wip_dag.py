from kaggle.api.kaggle_api_extended import KaggleApi
from pyspark.sql import SparkSession
from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
import os
# from updatescript import update
import pandas as pd
from datetime import datetime

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


# @task
# def read():
#     """
#     Function that identifies current directory, locates txt file created by previous task, and reads the content
#     """
#     path = os.path.abspath(__file__)
#     dir_name = os.path.dirname(path)
#     with open(f"{dir_name}/shape.txt", 'r') as txt:
#         return txt.read

@task
def update(shape_xcom):
    """
    Main function to create df of new data
    """
    spark = SparkSession.builder.getOrCreate()
    daily_df = spark.read.csv('Daily_Country_Wise_Confirmed_Cases.csv', header=True)
    daily_shape = (len(daily_df.columns), daily_df.count())
    if daily_shape == shape_xcom:
        pass
    elif daily_shape[0] == shape_xcom[1]:
        rows = daily_df.count() - shape_xcom[0]
        daily_df = daily_df.select('Country',daily_df[-rows])
        #create pandas df and set index
        pandas_df = daily_df.toPandas()
        pandas_df.set_index('Country', inplace=True)
        #transpose and reset index
        transpose_df = pandas_df.transpose()
        transpose_df.reset_index(inplace=True)
        #create spark df 
        spark_df=spark.createDataFrame(transpose_df)
        spark_df = spark_df.withColumnRenamed('index', 'Date')
        #write to csv? or update?
        spark_df.write.csv(f"Daily_CC_{date.today()}", header=True)
    else:
        rows = daily_df.count() - shape_xcom[0]
        # add_column(daily_df, rows)

# @task
# def load():
#     """
#     load to bigqquery
#     """

@task
def create_shape():
    """
    export shape of df
    """
    path = os.path.abspath(__file__)
    dir_name = os.path.dirname(path)
    df = pd.read_csv(f"{dir_name}/Daily_CC.csv")
    return df.shape


#set dag attributes
@dag(
    schedule_interval='@daily',
    start_date=datetime.utcnow(),
    catchup=False,
)
#function that calls all of our tasks to create dag
def monkeypox_dag():
    api_task = api_pull()
    update_task = update((53, 56))
    create_shape_task = create_shape()
    echo_to_file=BashOperator(
        task_id='echo_to_file',
        bash_command=f'echo {create_shape_task} > /opt/airflow/dags/shape.txt')
    api_task >> update_task >> create_shape_task >> echo_to_file

monkeypox_dag = monkeypox_dag()