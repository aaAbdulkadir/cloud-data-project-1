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
import polars as pl
from airflow.exceptions import AirflowSkipException, AirflowException
from airflow_to_aws import (
    upload_to_s3,
    retrieve_from_s3,
    load_to_rds,
    get_latest_file_from_s3
)

S3_STAGING_BUCKET = Variable.get("S3_STAGING_BUCKET", '')
AIRFLOW_HOME = '/home/ubuntu/airflow'
STAGING_DATA = AIRFLOW_HOME + '/staging_data'


def get_filename_template(dag_id: str, task_id: str, ts: str, file_extension: str) -> str:
    """Gets the filename template for a given task. It formulates the filename
    based on the current task to the next task, with its timestamp and directory.

    Args:
        dag_id (str): The ID of the DAG.
        task_id (str): The ID of the current task.
        ts (str): The timestamp of the task run.
        file_extension (str): The file extension of the output file.

    Returns:
        str: The formulated filename.
    """
    return f"{dag_id}/{dag_id}_{task_id}_{ts}.{file_extension}"



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
    logger = logging.getLogger('Load config if exists')
    if 'config_path' in dag_params:
        logger.info('config_path exists in yml file')
        config_path = os.path.join(scrape_dir_path, dag_params['config_path'])
        config = load_json_file(config_path)
    else:
        logger.info('config_path does not exist in yml file')
        config = {}
    return config


def check_two_dataframes(df_1: pl.DataFrame, df_2: pl.DataFrame) -> bool:
    """Compares two dataframes and checks if they are identical.

    Args:
        df_1 (pl.DataFrame): The newly extracted dataframe.
        df_2 (pl.DataFrame): The previously extracted dataframe.

    Returns:
        bool: True if the dataframes are different, False if they are the same.
    """

    if sorted(df_1.columns) != sorted(df_2.columns):
        raise AirflowException(
            f'Difference in columns detected: '
            f'Columns in df_1: {sorted(df_1.columns)}, '
            f'Columns in df_2: {sorted(df_2.columns)}'
        )

    cols_sorted_1 = sorted(df_1.columns)
    cols_sorted_2 = sorted(df_2.columns)
    
    df_1 = df_1.select(cols_sorted_1).sort(cols_sorted_1)
    df_2 = df_2.select(cols_sorted_2).sort(cols_sorted_2)
    
    if df_1.equals(df_2):
        raise AirflowSkipException(
            'Extracted file is the same as the previously extracted file, skipping.'
        )
    else:
        return True
    

def read_in_data(filename: str) -> pl.DataFrame:
    """Reads in data from a file and returns it as a Polars DataFrame.

    Args:
        filename (str): The path to the file.

    Raises:
        AirflowException: If the file format is unsupported.

    Returns:
        pl.DataFrame: The data as a Polars DataFrame.
    """
    extension = filename.split('.')[-1]
    if extension == 'csv':
        return pl.read_csv(filename)
    elif extension in ['xlsx', 'xls']:
        return pl.read_excel(filename)
    elif extension == 'parquet':
        return pl.read_parquet(filename)
    else:
        raise AirflowException(f"Unsupported file format: {extension}")


def task_wrapper(task_function: Callable, **kwargs) -> None:
    """
    Wraps a task function to handle file staging, S3 interactions, and Airflow XCom.

    This wrapper manages file paths, retrieves necessary files from S3, executes
    the given task function, optionally compares the output with the latest file
    from S3, and pushes the output filename to XCom for downstream tasks.

    Args:
        task_function (Callable): The main task function to be executed.
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
        file_extension (str): The file extension for the output file.
        latest_file_comparison_check (bool): Whether to perform a comparison with
            the latest file in the S3 bucket during the extract task.

    Raises:
        AirflowException: If attempting to extract a previously extracted filename.
        
    Workflow:
        - Determines file paths and S3 keys based on task details.
        - Pulls the previous task's output file from S3 if the current task is not an 'extract' task.
        - Constructs arguments for the provided `task_function` based on the task type.
        - Calls the provided `task_function` with the constructed arguments.
        - If the task is an 'extract' task, optionally compares the new file with the latest one from S3.
        - Uploads the output file to S3 if the task is not a 'load' task.
        - Pushes the output filename to XCom for downstream tasks.
    """
    logger = logging.getLogger('Task wrapper')
    
    ti = kwargs['ti']
    task_id = kwargs['task'].task_id
    dag_id = kwargs['dag'].dag_id
    ts = kwargs['ts']
    file_extension = kwargs['file_extension']

    output_filename = get_filename_template(dag_id, task_id, ts, file_extension)
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
    
    # Latest file comparison check after extract # previously extracted file if file name is same
    if 'extract' in task_id:
        latest_file_comparison_check = kwargs['latest_file_comparison_check']
        latest_s3_key = get_latest_file_from_s3(bucket=S3_STAGING_BUCKET, dag_id=dag_id, task_id=task_id)
        
        if latest_s3_key == local_output_filepath:
            raise AirflowException('Cannot extract previously extracted filename')
        elif latest_s3_key is None:
            logger.info('No latest file found in S3 bucket, skipping the comparison check.')
            latest_file_comparison_check = False
        
        if latest_file_comparison_check is True:
            logger.info('Carrying out latest file comparison check')
            latest_local_file_path = f"{STAGING_DATA}/{latest_s3_key.split('/')[-1]}"
            retrieve_from_s3(bucket=S3_STAGING_BUCKET, s3_key=latest_s3_key, local_file_path=latest_local_file_path)
            
            df_1 = read_in_data(local_output_filepath)
            df_2 = read_in_data(latest_local_file_path)
            if check_two_dataframes(df_1, df_2):
                os.remove(latest_local_file_path)
                
                    
    # Upload output file to S3 staging bucket
    if 'load' not in task_id:
        upload_to_s3(local_file_path=local_output_filepath, bucket=S3_STAGING_BUCKET, s3_key=s3_key)
        
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
    }
    
    schedule_interval = dag_params.get('schedule_interval')
    if schedule_interval == 'None':
        schedule_interval = None
        
    dag = DAG(
        dag_id,
        default_args=default_args,
        description=dag_params.get('description', ''),
        schedule_interval=schedule_interval,
        start_date=pendulum.parse(str(dag_params.get('start_date', pendulum.now('UTC')))),
        catchup=dag_params.get('catchup', False),
    )

    with dag:
        tasks = {}

        for task_id, task_params in dag_params.get('tasks', {}).items():
            task_params = dag_params['tasks'][task_id]

            if 'load' in task_id:
                python_callable = load_to_rds
            else:
                python_callable = getattr(functions, task_params.get('python_callable'))
                
            if 'extract' in task_id:
                file_extension = task_params.get('file_extension', 'csv')
            else:
                file_extension = 'csv'

            args = {}

            # Get the previous task from dependencies
            dependencies = task_params.get('dependencies', [])
            prev_task_id = dependencies[0] if dependencies else None

            if 'extract' in task_id:
                args.update({
                    'url': dag_params.get('url'),
                    'output_filename': get_filename_template(dag_id, task_id, '{{ ts }}', file_extension),
                    'logical_timestamp': '{{ ts }}',
                    'config': config,
                    'params': task_params.get('params', {}),
                    'file_extension': file_extension,
                    'latest_file_comparison_check': task_params.get('latest_file_comparison_check', False),
                })
            elif 'load' in task_id:
                args.update({
                    'input_filename': "{{ ti.xcom_pull(task_ids='" + prev_task_id + "', key='output_filename') }}",
                    'mode': task_params.get('mode'),
                    'dataset_name': task_params.get('dataset_name'),
                    'fields': task_params.get('fields'),
                    'file_extension': file_extension,
                })
            else:
                args.update({
                    'input_filename': "{{ ti.xcom_pull(task_ids='" + prev_task_id + "', key='output_filename') }}",
                    'output_filename': get_filename_template(dag_id, task_id, '{{ ts }}', file_extension),
                    'config': config,
                    'params': task_params.get('params', {}),
                    'file_extension': file_extension,
                })

            task = PythonOperator(
                task_id=task_id,
                python_callable=task_wrapper,
                op_kwargs={'task_function': python_callable, 'prev_task_id': prev_task_id, **args,},
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
                # Update next_task_id for the dependency task
                tasks[dependency].op_kwargs['next_task_id'] = task_id

    return dag
