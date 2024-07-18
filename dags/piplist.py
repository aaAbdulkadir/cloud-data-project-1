from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 7, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'pip_list_dag',
    default_args=default_args,
    description='A simple DAG to run pip list using BashOperator',
    schedule_interval=timedelta(days=1),
)

# Define the task using BashOperator
pip_list_task = BashOperator(
    task_id='pip_list',
    bash_command='pip list',
    dag=dag,
)

# Set task dependencies (if any)
# For a single task DAG, this is not necessary
pip_list_task
