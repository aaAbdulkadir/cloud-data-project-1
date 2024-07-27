import os
import yaml
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import pendulum
import importlib
import json
from datetime import timedelta

AIRFLOW_HOME = '/home/ubuntu/airflow'
STAGING_DATA = AIRFLOW_HOME + '/staging_data/'

EXTRACT_TO_TRANSFROM = (
    STAGING_DATA + '{{ dag.dag_id }}_extract_to_transform_{{ ts }}.{{ dag.default_args.file_extension }}'
)
TRANSFORM_TO_LOAD = (
    STAGING_DATA + '{{ dag.dag_id }}_transform_to_load_{{ ts }}.csv'
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

def create_dag(scrape_dir_path: str) -> DAG:
    """Create a DAG from the configuration and functions in the specified directory."""

    scrape_dir  = os.path.basename(yml_file_path)
    yml_file_path = os.path.join(scrape_dir_path, f'{scrape_dir}.yml')
    dag_id = os.path.basename(yml_file_path).split('.')[0]
    dag_params = load_yml_file(yml_file_path)[dag_id]
    load_dag_filepath = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'load_to_db.py')

    # Load functions from the specified functions file
    functions_filepath = os.path.join(scrape_dir_path, dag_params['python_callable_file'])
    functions = import_functions(functions_filepath)
    load_to_db = import_functions(load_dag_filepath)

    config = {}
    if 'config_path' in dag_params:
        config_path = os.path.join(scrape_dir_path, dag_params['config_path'])
        config = load_json_file(config_path)

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
                python_callable = getattr(load_to_db, 'load')
            else:
                python_callable = getattr(functions, task_params.get('python_callable'))

            task_kwargs = {
                **task_params.get('kwargs', {}),  
            }
            
            if task_id == 'extract':
                task_kwargs.update({
                    'url': dag_params.get('url'),
                    'output_filename': EXTRACT_TO_TRANSFROM,
                    'logical_timestamp': '{{ ts }}',
                    'config': config,
                })
            elif task_id == 'transform':
                task_kwargs.update({
                    'input_filename': EXTRACT_TO_TRANSFROM,
                    'output_filename': TRANSFORM_TO_LOAD,
                })
            elif task_id == 'load':
                task_kwargs.update({
                    'input_filename': TRANSFORM_TO_LOAD,
                    'mode': task_params.get('mode'),
                    'dataset_name': task_params.get('dataset_name'),
                    'fields': task_params.get('fields'),
                })

            task = PythonOperator(
                task_id=task_id,
                python_callable=python_callable,
                retries=task_params.get('retries', 0),
                retry_delay=timedelta(seconds=task_params.get('retry_delay', 15)),
                op_kwargs=task_kwargs,
                dag=dag
            )
            tasks[task_id] = task

        for task_id, task_params in dag_params.get('tasks', {}).items():
            dependencies = task_params.get('dependencies', [])
            for dependency in dependencies:
                tasks[dependency] >> tasks[task_id]

    return dag
