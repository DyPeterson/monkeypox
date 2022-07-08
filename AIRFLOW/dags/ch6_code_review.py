import time
import random
import os
import re
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.decorators import dag, task

APPLES = ["pink lady", "jazz", "orange pippin", "granny smith", "red delicious", "gala", "honeycrisp", "mcintosh", "fuji"]

def print_hello():
    """
    Function that identifies current directory, locates txt file created by previous task, and reads the content
    """
    path = os.path.abspath(__file__)
    dir_name = os.path.dirname(path)
    with open(f"{dir_name}/ch6_code_review.txt", 'r') as txt:
        print(f'Howdy, {txt.read()}!')

def pick_apple(apple: str):
    """
    Selects a random choice from our APPLES list
    """
    vowel = re.search('^[aeiou]', apple)
    if vowel:
        print(f"Yum! An {apple}")
    else:
        print(f"Yum! A {apple}")

#establish our default args
default_args = {
    'schedule_interval':'@once',
    'start_date': datetime.utcnow(),
    'catchup': False
} 
#instantiate our DAG
with DAG(
    'apple_picking',
    description='An example DAG that print user name, three randomly selected apples.',
    default_args=default_args
) as dag:

    #Bash task that creates a txt file 
    echo_to_file=BashOperator(
        task_id='echo_to_file',
        bash_command='echo Farmer > /opt/airflow/dags/ch6_code_review.txt'
    )
    #Python task that calls our print_hello function
    print_hello = PythonOperator(
        task_id='print_hello',
        python_callable=print_hello
    )
    #prints to console
    now_picking=BashOperator(
        task_id='now_picking',
        bash_command='echo Picking three random apples...'
    )
    #creates our three simultaneous tasks
    apple_tasks = []
    #range(1,4) so our tasks are labeled 1,2 and 3
    for i in range(1,4):
        apple_choice = random.choice(APPLES)
        task= PythonOperator(
        #name our tasks
        task_id=f'apple_{i}',
        python_callable=pick_apple,
        op_kwargs={'apple': apple_choice}
    )
        apple_tasks.append(task)
    #finish our DAG with an empty operator to ensure all previous tasks have run
    all_done = EmptyOperator(task_id='all_done')
    #establish our task order
    echo_to_file >> print_hello >> now_picking >> apple_tasks >> all_done

