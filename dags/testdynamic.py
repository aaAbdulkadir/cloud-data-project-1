import os
import yaml
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pendulum

def load_yml_file(yml_file_path: str) -> dict:
    """Load DAG configuration from a YAML file."""
    with open(yml_file_path, 'r') as config_file:
        return yaml.safe_load(config_file)

# Dummy functions to simulate actual tasks
def dummy_extract_function(**kwargs):
    print("Running extract function")

def dummy_transform_function(**kwargs):
    print("Running transform function")

def dummy_load_function(**kwargs):
    print("Running load function")

# Dictionary to simulate function imports
functions_dict = {
    'extract_function': dummy_extract_function,
    'transform_function': dummy_transform_function,
    'load_function': dummy_load_function,
    'extract_taxi_data': dummy_extract_function,
    'transform_taxi_data': dummy_transform_function,
    'load_taxi_data': dummy_load_function
}

def create_dag(dag_id: str, dag_params: dict) -> DAG:
    """Create a DAG from the configuration and functions in the specified directory."""
    
    default_args = {
        'owner': dag_params.get('owner', 'airflow'),
        'depends_on_past': False,
        'description': dag_params.get('description', ''),
        'email_on_failure': dag_params.get('email_on_failure', False),
        'email_on_success': dag_params.get('email_on_success', False),
        'email_on_retry': dag_params.get('email_on_retry', False),
        'email': [dag_params.get('email')],
    }

    dag = DAG(
        dag_id,
        default_args=default_args,
        schedule_interval=dag_params.get('schedule_interval'),
        start_date=pendulum.now('UTC'),
        catchup=dag_params.get('catchup', False),
    )

    with dag:
        tasks = {}

        for task_id, task_params in dag_params.get('tasks', {}).items():
            python_callable = functions_dict.get(task_params.get('python_callable'))
            task_kwargs = task_params.get('kwargs', {})

            task = PythonOperator(
                task_id=task_id,
                python_callable=python_callable,
                retries=task_params.get('retries', 1),
                retry_delay=timedelta(seconds=task_params.get('retry_delay', 30)),
                op_kwargs=task_kwargs,
                dag=dag
            )
            tasks[task_id] = task

        # Define task dependencies
        if 'extract' in tasks and 'transform' in tasks:
            tasks['extract'] >> tasks['transform']
        if 'transform' in tasks and 'load' in tasks:
            tasks['transform'] >> tasks['load']

    return dag

# Paths to the YAML files
yaml_file_paths = [
    'dummy_dag/dummy_dag.yml',
    'new_york_taxi/new_york_taxi.yml'
]

# Create and register the DAGs
for yaml_file_path in yaml_file_paths:
    if os.path.isfile(yaml_file_path):
        try:
            dag_params = load_yml_file(yaml_file_path)
            dag_id = os.path.basename(os.path.dirname(yaml_file_path))
            dag_params = dag_params[dag_id]
            globals()[dag_id] = create_dag(dag_id, dag_params)
            print(f"Registered DAG: {dag_id}")
        except Exception as e:
            print(f"Failed to create DAG from {yaml_file_path}: {e}")
