import os
import yaml
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import pendulum
import importlib
import json
from datetime import timedelta
from airflow.models import Variable
from typing import Callable
import logging
import inspect

from airflow_to_aws import (
    upload_to_s3,
    retrieve_from_s3,
    load_to_rds,
)

S3_STAGING_BUCKET = Variable.get("S3_STAGING_BUCKET", '')
AIRFLOW_HOME = '/home/ubuntu/airflow'
STAGING_DATA = AIRFLOW_HOME + '/staging_data'


def get_filename_template(dag_id: str, task_id: str, next_task_id: str, ts: str, file_extension: str) -> str:
    """Gets the filename template for a given task. It formulates the filename
    based on the current task to the next task, with its timestamp and directory.

    Args:
        dag_id (str): The ID of the DAG.
        task_id (str): The ID of the current task.
        next_task_id (str): The ID of the next task.
        ts (str): The timestamp of the task run.
        file_extension (str): The file extension of the output file.

    Returns:
        str: The formulated filename.
    """
    return f"{dag_id}/{dag_id}_{task_id}_to_{next_task_id}_{ts}.{file_extension}"



def load_yml_file(yml_file_path: str) -> dict:
    """Loads the yml file

    Args:
        yml_file_path (str): The file path of the yml configuration file.

    Returns:
        dict: The yml file parameters.
    """
    with open(yml_file_path, 'r') as config_file:
        return yaml.safe_load(config_file)
    

def load_json_file(json_file_path: str) -> dict:
    """Loads the json file

    Args:
        json_file_path (str): The file path of the json configuration file.

    Returns:
        dict: The json file parameters.
    """
    with open(json_file_path, 'r') as json_file:
        return json.load(json_file)
    

def import_functions(functions_filepath: str) -> object:
    """Imports functions from another Python file.

    Args:
        functions_filepath (str): The file path of the Python file containing the functions.

    Returns:
        object: The module containing the imported functions.
    """
    spec = importlib.util.spec_from_file_location("functions_module", functions_filepath)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def load_config_if_exists(scrape_dir_path: str, dag_params: dict) -> dict:
    """Load a configuration file if it exists.

    This function checks if a configuration file path is specified in the `dag_params`. 
    If the path exists, it loads the configuration from the specified JSON file.
    If the path does not exist, it returns an empty dictionary.

    Args:
        scrape_dir_path (str): The directory path where the configuration file is located.
        dag_params (dict): The parameters of the DAG, which may include the path to the 
                           configuration file.

    Returns:
        dict: The loaded configuration as a dictionary. If no configuration file is found, 
              an empty dictionary is returned.
    """
    logger = logging.getLogger('Load config if exists'
                               )
    if 'config_path' in dag_params:
        logger.info('config_path exists in yml file')
        config_path = os.path.join(scrape_dir_path, dag_params['config_path'])
        config = load_json_file(config_path)
    else:
        logger.info('config_path does not exist in yml file')
        config = {}
    return config


def task_wrapper(task_function: Callable, next_task_id: str, **kwargs) -> None:
    """Wraps a task function to handle file staging, S3 interactions, and Airflow XCom.

    This wrapper handles the file paths, pulls necessary files from S3, executes
    the given task function, and pushes the output filename to XCom for downstream tasks.

    Args:
        task_function (Callable): The main task function to be executed.
        next_task_id (str): The task ID of the next task in the DAG.
        **kwargs: Additional keyword arguments passed by Airflow.

    Keyword Args:
        ti (TaskInstance): The Airflow TaskInstance object.
        task (Task): The Airflow Task object.
        dag (DAG): The Airflow DAG object.
        ts (str): The timestamp of the DAG run.
        prev_task_id (str): The task ID of the previous task in the DAG.
        url (str): The URL to be used by the extract task.
        logical_timestamp (str): The logical timestamp for the extract task.
        config (dict): Configuration dictionary for the extract task.
        dataset_name (str): The name of the dataset for the load task.
        mode (str): The mode (append/replace) for the load task.
        fields (list): The key fields for the load task.
    """ 
    logger = logging.getLogger('Task wrapper')
    
    ti = kwargs['ti']
    task_id = kwargs['task'].task_id
    dag_id = kwargs['dag'].dag_id
    ts = kwargs['ts']
    file_extension = kwargs['dag'].default_args['file_extension']

    output_filename = get_filename_template(dag_id, task_id, next_task_id, ts, file_extension)
    local_output_filepath = f"{STAGING_DATA}/{output_filename}"
        
    directory = f"{STAGING_DATA}/{dag_id}"
    s3_key = output_filename
    os.makedirs(directory, exist_ok=True)
    
    # Pull file from S3 staging bucket if not 'extract' task
    if 'extract' not in task_id:
        input_s3_key = ti.xcom_pull(task_ids=kwargs['prev_task_id'], key='output_filename')
        input_local_filepath = f"{STAGING_DATA}/{input_s3_key}"
        retrieve_from_s3(bucket=S3_STAGING_BUCKET, s3_key=input_s3_key, local_file_path=input_local_filepath)
    else:
        input_local_filepath = None
    
    # Construct arguments for the task function
    task_function_params = inspect.signature(task_function).parameters
    task_args = {}
    
    if 'extract' in task_id:
        task_args['url'] = kwargs['url']
        task_args['output_filename'] = local_output_filepath
        if 'logical_timestamp' in task_function_params:
            task_args['logical_timestamp'] = pendulum.parse(kwargs['ts'])
        if 'config' in task_function_params:
            task_args['config'] = kwargs['config']
        if 'params' in task_function_params:
            task_args['params'] = kwargs['params']
    elif 'load' in task_id:
        task_args['dataset_name'] = kwargs['dataset_name']
        task_args['input_filename'] = input_local_filepath
        task_args['mode'] = kwargs['mode']
        task_args['fields'] = kwargs['fields']
    else:
        task_args['input_filename'] = input_local_filepath
        task_args['output_filename'] = local_output_filepath
        if 'config' in task_function_params:
            task_args['config'] = kwargs['config']
        if 'params' in task_function_params:
            task_args['params'] = kwargs['params']
        
    # Call the main Python callable with the constructed arguments
    task_function(**task_args)
        
    # Upload output file to S3 staging bucket
    if 'load' not in task_id:
        upload_to_s3(local_file_path=local_output_filepath, bucket=S3_STAGING_BUCKET, s3_key=s3_key)
        
    # after downloading the fiel from s3
    if 'extract' not in task_id:
        logger.info(f'Removed {input_local_filepath}')
        os.remove(input_local_filepath)
        
        
    ti.xcom_push(key='output_filename', value=output_filename)
    

def create_dag(yml_file_path: str) -> DAG:
    """Create a DAG from the configuration and functions specified in a YAML file.

    This function reads the DAG configuration from a YAML file, loads the necessary
    Python functions from a specified file, and constructs an Airflow DAG with the 
    tasks and dependencies as defined in the YAML file.

    Args:
        yml_file_path (str): The path to the YAML file containing the DAG configuration.

    Returns:
        DAG: The constructed Airflow DAG.
    """
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
        'email_on_failure': dag_params.get('email_on_failure', False),
        'email_on_success': dag_params.get('email_on_success', False),
        'email_on_retry': dag_params.get('email_on_retry', False),
        'email': [dag_params.get('email')],
        'file_extension': dag_params.get('tasks', {}).get('extract', {}).get('file_extension', 'csv')
    }

    dag = DAG(
        dag_id,
        default_args=default_args,
        description=dag_params.get('description', ''),
        schedule_interval=dag_params.get('schedule_interval'),
        start_date=pendulum.parse(str(dag_params.get('start_date', pendulum.now('UTC')))),
        catchup=dag_params.get('catchup', False),
    )

    with dag:
        tasks = {}

        task_order = list(dag_params.get('tasks', {}).keys())

        for idx, task_id in enumerate(task_order):
            task_params = dag_params['tasks'][task_id]

            if 'load' in task_id:
                python_callable = load_to_rds
            else:
                python_callable = getattr(functions, task_params.get('python_callable'))

            args = {}

            next_task_id = task_order[idx + 1] if idx + 1 < len(task_order) else ''
            prev_task_id = task_order[idx - 1] if idx > 0 else None

            if 'extract' in task_id:
                args.update({
                    'url': dag_params.get('url'),
                    'output_filename': get_filename_template(dag_id, task_id, next_task_id, '{{ ts }}', '{{ dag.default_args.file_extension }}'),
                    'logical_timestamp': '{{ ts }}',
                    'config': config,
                    'params': task_params.get('params', {})
                })
            elif 'load' in task_id:
                args.update({
                    'input_filename': "{{ ti.xcom_pull(task_ids='" + prev_task_id + "', key='output_filename') }}",
                    'mode': task_params.get('mode'),
                    'dataset_name': task_params.get('dataset_name'),
                    'fields': task_params.get('fields'),
                })
            else:
                args.update({
                    'input_filename': "{{ ti.xcom_pull(task_ids='" + prev_task_id + "', key='output_filename') }}",
                    'output_filename': get_filename_template(dag_id, task_id, next_task_id, '{{ ts }}', '{{ dag.default_args.file_extension }}'),
                    'config': config,
                    'params': task_params.get('params', {})
                })

            task = PythonOperator(
                task_id=task_id,
                python_callable=task_wrapper,
                op_kwargs={'task_function': python_callable, 'next_task_id': next_task_id, 'prev_task_id': prev_task_id, **args,},
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
