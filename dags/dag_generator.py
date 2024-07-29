import os
import yaml
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import pendulum
import importlib
import json
from datetime import timedelta

from airflow_to_aws import (
    upload_to_s3,
    load_to_rds,
    retrieve_from_s3,
)

def load_yml_file(yml_file_path: str) -> dict:
    """Load DAG configuration from a YAML file."""
    with open(yml_file_path, 'r') as config_file:
        return yaml.safe_load(config_file)

def load_json_file(json_file_path: str) -> dict:
    """Load JSON configuration file."""
    with open(json_file_path, 'r') as json_file:
        return json.load(json_file)

def import_functions(functions_filepath: str):
    """Import Python functions from the specified filepath."""
    spec = importlib.util.spec_from_file_location("functions_module", functions_filepath)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module

def load_config_if_exists(scrape_dir_path: str, dag_params: dict) -> dict:
    """Load configuration file if it exists."""
    if 'config_path' in dag_params:
        config_path = os.path.join(scrape_dir_path, dag_params['config_path'])
        config = load_json_file(config_path)
    else:
        config = None
    return config

def task_wrapper(task_function, **kwargs):
    ti = kwargs['ti']
    task_id = kwargs['task'].task_id

    if task_id == 'extract':
        output_filename = kwargs['output_filename']
        url = kwargs['url']
        logical_timestamp = kwargs['logical_timestamp']
        config = kwargs['config']
        historical = kwargs.get('historical')
        task_function(url=url, output_filename=output_filename, logical_timestamp=logical_timestamp, config=config, historical=historical)
    elif task_id == 'transform':
        input_filename = kwargs['input_filename']
        output_filename = kwargs['output_filename']
        task_function(input_filename=input_filename, output_filename=output_filename)
    elif task_id == 'load':
        dataset_name = kwargs['dataset_name']
        input_filename = kwargs['input_filename']
        mode = kwargs['mode']
        keyfields = kwargs['keyfields']
        task_function(dataset_name=dataset_name, input_filename=input_filename, mode=mode, keyfields=keyfields)

    if 'output_filename' in kwargs:
        ti.xcom_push(key='output_filename', value=kwargs['output_filename'])

def create_dag(yml_file_path: str) -> DAG:
    """Create a DAG from the configuration and functions in the specified directory."""
    dag_id = os.path.basename(yml_file_path).split('.')[0]
    dag_params = load_yml_file(yml_file_path)[dag_id]
    scrape_dir_path = os.path.dirname(yml_file_path)

    # Load functions from the specified functions file
    functions_filepath = os.path.join(scrape_dir_path, dag_params['python_callable_file'])
    functions = import_functions(functions_filepath)

    config = load_config_if_exists(scrape_dir_path, dag_params)

    default_args = {
        'owner': dag_params.get('owner', 'airflow'),
        'depends_on_past': False,
        'description': dag_params.get('description', ''),
        'email_on_failure': dag_params.get('email_on_failure', False),
        'email_on_success': dag_params.get('email_on_success', False),
        'email_on_retry': dag_params.get('email_on_retry', False),
        'email': [dag_params.get('email')],
        'file_extension': dag_params.get('tasks', {}).get('extract', {}).get('file_extension', 'csv')
    }

    dag = DAG(
        dag_id,
        default_args=default_args,
        schedule_interval=dag_params.get('schedule_interval'),
        start_date=dag_params.get('start_date', pendulum.now('UTC')),
        catchup=dag_params.get('catchup', False),
    )

    with dag:
        tasks = {}

        for task_id, task_params in dag_params.get('tasks', {}).items():
            if task_id == 'load':
                python_callable = load_to_rds
            else:
                python_callable = getattr(functions, task_params.get('python_callable'))

            task_kwargs = {}

            task_kwargs = {
                **task_params.get('kwargs', {}),  
            }

            if task_id == 'extract':
                task_kwargs.update({
                    'url': dag_params.get('url'),
                    'output_filename': f"{dag_id}_extract_{pendulum.now().to_iso8601_string()}.csv",
                    'logical_timestamp': '{{ ts }}',
                    'config': config,
                })
            elif task_id == 'transform':
                task_kwargs.update ({
                    'input_filename': f"{dag_id}_extract_{pendulum.now().to_iso8601_string()}.csv",
                    'output_filename': f"{dag_id}_transform_{pendulum.now().to_iso8601_string()}.csv"
                })
            elif task_id == 'load':
                task_kwargs.update({
                    'input_filename': f"{dag_id}_transform_{pendulum.now().to_iso8601_string()}.csv",
                    'mode': task_params.get('mode'),
                    'dataset_name': task_params.get('dataset_name'),
                    'keyfields': task_params.get('fields'),
                })

            task = PythonOperator(
                task_id=task_id,
                python_callable=task_wrapper,
                op_kwargs={**task_kwargs, 'task_function': python_callable},
                retries=task_params.get('retries', 0),
                retry_delay=timedelta(seconds=task_params.get('retry_delay', 15)),
                provide_context=True,
                dag=dag
            )
            tasks[task_id] = task

        for task_id, task_params in dag_params.get('tasks', {}).items():
            dependencies = task_params.get('dependencies', [])
            for dependency in dependencies:
                tasks[dependency] >> tasks[task_id]

    return dag

