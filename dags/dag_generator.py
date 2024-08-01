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
    retrieve_from_s3,
    load_to_rds,
)

S3_STAGING_BUCKET = os.getenv('S3_STAGING_BUCKET')
AIRFLOW_HOME = '/home/ubuntu/airflow'
STAGING_DATA = AIRFLOW_HOME + '/staging_data'

def get_filename_template(dag_id, task_id, next_task_id, ts, file_extension):
    return f"{STAGING_DATA}/{dag_id}_{task_id}_to_{next_task_id}_{ts}.{file_extension}"

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
    """Load configuration file if it exists."""#
    config = {}
    if 'config_path' in dag_params:
        config_path = os.path.join(scrape_dir_path, dag_params['config_path'])
        config = load_json_file(config_path)
    return config

# def load_in_params(task_params: dict) -> dict:
#     """Load additional parameters if they exist."""
#     params = {}
#     if 'params' in task_params:
#         params = task_params['params']
#     return params

 ## PARAMS ISSUE FIX
def task_wrapper(task_function, next_task_id, **kwargs):
    ti = kwargs['ti']
    task_id = kwargs['task'].task_id
    dag_id = kwargs['dag'].dag_id
    ts = kwargs['ts']
    file_extension = kwargs['dag'].default_args['file_extension']

    output_filename = get_filename_template(dag_id, task_id, next_task_id, ts, file_extension)

    if task_id == 'extract':
        url = kwargs['url']  
        logical_timestamp = kwargs['logical_timestamp']
        config = kwargs.get('config', {})
        task_function(url=url, output_filename=output_filename, logical_timestamp=logical_timestamp, config=config, **kwargs)
    elif task_id == 'load':
        dataset_name = kwargs['dataset_name']
        input_filename = kwargs['input_filename']
        mode = kwargs['mode']
        keyfields = kwargs['keyfields']
        task_function(dataset_name=dataset_name, input_filename=input_filename, mode=mode, keyfields=keyfields)
    else:
        input_filename = kwargs['input_filename']
        task_function(input_filename=input_filename, output_filename=output_filename, **kwargs)

    ti.xcom_push(key='output_filename', value=output_filename)

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

        task_order = list(dag_params.get('tasks', {}).keys())

        for idx, task_id in enumerate(task_order):
            task_params = dag_params['tasks'][task_id]

            if task_id == 'load':
                python_callable = load_to_rds
            else:
                python_callable = getattr(functions, task_params.get('python_callable'))
                # params = load_in_params(task_params)

            task_kwargs = {**task_params.get('params', {})}

            next_task_id = task_order[idx + 1] if idx + 1 < len(task_order) else ''

            if task_id == 'extract':
                task_kwargs.update({
                    'url': dag_params.get('url'),
                    'output_filename': get_filename_template(dag_id, task_id, next_task_id, '{{ ts }}', '{{ dag.default_args.file_extension }}'),
                    'logical_timestamp': '{{ ts }}',
                    'config': config,
                    # 'params': params
                })
            elif task_id == 'load':
                previous_task_id = task_order[idx - 1]
                task_kwargs.update({
                    'input_filename': "{{ ti.xcom_pull(task_ids='" + previous_task_id + "', key='output_filename') }}",
                    'mode': task_params.get('mode'),
                    'dataset_name': task_params.get('dataset_name'),
                    'keyfields': task_params.get('fields'),
                })
            else:
                previous_task_id = task_order[idx - 1]
                task_kwargs.update({
                    'input_filename': "{{ ti.xcom_pull(task_ids='" + previous_task_id + "', key='output_filename') }}",
                    'output_filename': get_filename_template(dag_id, task_id, next_task_id, '{{ ts }}', '{{ dag.default_args.file_extension }}'),
                    # 'params': params
                })

            task = PythonOperator(
                task_id=task_id,
                python_callable=task_wrapper,
                op_kwargs={**task_kwargs, 'task_function': python_callable, 'next_task_id': next_task_id},
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
