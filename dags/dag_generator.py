import os
import yaml
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import pendulum
import importlib
import json
from datetime import timedelta

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

    dag_id = os.path.basename(scrape_dir_path)
    yml_file_path = os.path.join(scrape_dir_path, f'{dag_id}.yml')
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

    file_extension = dag_params.get('tasks', {}).get('extract', {}).get('file_extension', 'csv')

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
        start_date=dag_params.get('start_date', pendulum.now('UTC')),
        catchup=dag_params.get('catchup', False),
    )

    with dag:
        # Define the extract task
        extract_task = PythonOperator(
            task_id='extract',
            python_callable=functions.extract_function,
            retries=dag_params['tasks']['extract'].get('retries', 0),
            retry_delay=timedelta(seconds=dag_params['tasks']['extract'].get('retry_delay', 15)),
            op_kwargs={
                'url': dag_params.get('url'),
                'output_filename': f'{dag_id}_extract_to_transform_{{{{ ts }}}}.{file_extension}',
                'logical_timestamp': '{{ ts }}',
                **config,
                **dag_params['tasks']['extract'].get('kwargs', {})
            },
            dag=dag
        )

        # Define the transform task
        transform_task = PythonOperator(
            task_id='transform',
            python_callable=functions.transform_function,
            retries=dag_params['tasks']['transform'].get('retries', 0),
            retry_delay=timedelta(seconds=dag_params['tasks']['transform'].get('retry_delay', 15)),
            op_kwargs={
                'input_filename': f'{dag_id}_extract_to_transform_{{{{ ts }}}}.{file_extension}',
                'output_filename': f'{dag_id}_transform_to_load_{{{{ ts }}}}.csv',
                **config,
                **dag_params['tasks']['transform'].get('kwargs', {})
            },
            dag=dag
        )

        # Define the load task
        load_task = PythonOperator(
            task_id='load',
            python_callable=load_to_db.load_function,
            retries=dag_params['tasks']['load'].get('retries', 0),
            retry_delay=timedelta(seconds=dag_params['tasks']['load'].get('retry_delay', 15)),
            op_kwargs={
                'input_filename': f'{dag_id}_transform_to_load_{{{{ ts }}}}.csv',
                'mode': dag_params['tasks']['load'].get('mode'),
                'dataset_name': dag_params['tasks']['load'].get('dataset_name'),
                'fields': dag_params['tasks']['load'].get('fields'),
                **config,
                **dag_params['tasks']['load'].get('kwargs', {})
            },
            dag=dag
        )

        # Set task dependencies
        extract_task >> transform_task >> load_task

    return dag