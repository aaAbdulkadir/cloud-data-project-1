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
)

AIRFLOW_HOME = '/home/ubuntu/airflow'
STAGING_DATA = AIRFLOW_HOME + '/staging_data/'

EXTRACT_TO_TRANSFROM = (
    STAGING_DATA + '{{ dag.dag_id }}_extract_to_transform_{{ ts }}.{{ dag.default_args.file_extension }}'
)
TRANSFORM_TO_LOAD = (
    STAGING_DATA + '{{ dag.dag_id }}_transform_to_load_{{ ts }}.csv'
)
S3_STAGING_BUCKET = os.getenv('S3_STAGING_BUCKET')


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

def create_dag(yml_file_path: str) -> DAG:
    """Create a DAG from the configuration and functions in the specified directory."""
    dag_id = os.path.basename(yml_file_path).split('.')[0]
    dag_params = load_yml_file(yml_file_path)[dag_id]
    scrape_dir_path = os.path.dirname(yml_file_path)

    # Load functions from the specified functions file
    functions_filepath = os.path.join(scrape_dir_path, dag_params['python_callable_file'])
    functions = import_functions(functions_filepath)

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
        extract = PythonOperator(
            task_id='extract',
            python_callable=getattr(functions, dag_params['tasks']['extract']['python_callable']),
            op_kwargs={
                'url': dag_params.get('url'),
                'output_filename': EXTRACT_TO_TRANSFROM,
                'logical_timestamp': '{{ ts }}',
                'config': config,
                **dag_params['tasks']['extract'].get('kwargs', {})
            },
            retries=dag_params['tasks']['extract'].get('retries', 0),
            retry_delay=timedelta(seconds=dag_params['tasks']['extract'].get('retry_delay', 15)),
        )

        extract_to_s3 = PythonOperator(
            task_id='extract_to_s3',
            python_callable=upload_to_s3,
            op_kwargs={
                'local_file_path': EXTRACT_TO_TRANSFROM,
                'bucket': S3_STAGING_BUCKET,
                's3_key': dag_id, 
            },
            retries=0
        )

        transform = PythonOperator(
            task_id='transform',
            python_callable=getattr(functions, dag_params['tasks']['transform']['python_callable']),
            op_kwargs={
                'input_filename': EXTRACT_TO_TRANSFROM,
                'output_filename': TRANSFORM_TO_LOAD,
                'config': config,
                **dag_params['tasks']['transform'].get('kwargs', {})
            },
            retries=dag_params['tasks']['transform'].get('retries', 0),
            retry_delay=timedelta(seconds=dag_params['tasks']['transform'].get('retry_delay', 15)),
        )

        transform_to_s3 = PythonOperator(
            task_id='transform_to_s3',
            python_callable=upload_to_s3,
            op_kwargs={
                'local_file_path': TRANSFORM_TO_LOAD,
                'bucket': S3_STAGING_BUCKET,
                's3_key': dag_id, 
            },
            retries=0
        )

        load = PythonOperator(
            task_id='load',
            python_callable=load_to_rds,
            op_kwargs={
                'input_filename': TRANSFORM_TO_LOAD,
                'mode': dag_params.get('tasks', {}).get('load', {}).get('mode'),
                'dataset_name': dag_params.get('tasks', {}).get('load', {}).get('dataset_name'),
                'fields': dag_params.get('tasks', {}).get('load', {}).get('fields'),                
            },
            retries=dag_params['tasks']['load'].get('retries', 0),
            retry_delay=timedelta(seconds=dag_params['tasks']['load'].get('retry_delay', 15)),
        )

        extract >> extract_to_s3 >> transform >> transform_to_s3 >> load

    return dag
