import os
import yaml
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import pendulum
import importlib
import json

def load_dag_config(yml_file_path: str) -> dict:
    """
    Load DAG configuration from a YAML file.

    Args:
        yml_file_path (str): Path to the YAML configuration file.

    Returns:
        dict: Loaded configuration as a dictionary.
    """
    with open(yml_file_path, 'r') as config_file:
        return yaml.safe_load(config_file)
    
def load_json_file(json_file_path: str) -> dict:
    """_summary_

    Args:
        json_file_path (str): _description_

    Returns:
        dict: _description_
    """
    with open(json_file_path, 'r') as json_file:
        return json.load(json_file)
    
    
def import_functions(scrape_dir_path: str):
    """_summary_

    Args:
        scrape_dir_path (_type_): _description_

    Returns:
        _type_: _description_
    """
    scrape_name = os.path.basename(scrape_dir_path)
    functions_path = os.path.join(scrape_dir_path, f'{scrape_name}_functions.py')
    module_name = f"dags.{scrape_name}.{scrape_name}_functions"
    spec = importlib.util.spec_from_file_location(module_name, functions_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def create_dag(scrape_dir_path: str) -> DAG:
    """_summary_

    Args:
        scrape_dir_name (_type_): _description_
    """
    dag_id = os.path.basename(scrape_dir_path)
    yml_file_path = scrape_dir_path + '/' + dag_id + '.yml'
    config = load_dag_config(yml_file_path)
    functions = import_functions(scrape_dir_path)
    dag_params = config[dag_id]

    # extra params
    file_extension = dag_params.get('tasks').get('extract').get('file_extension', 'csv')
    
    json_config_path = dag_params.get('json_config_path', False)
    if json_config_path:
        json_config_file_path = os.path.join(scrape_dir_path, 'config/config.json')
        config = load_json_file(json_config_file_path)


    default_args = {
        'owner': dag_params.get('owner', 'airflow'),
        'depends_on_past': False,
        'description': dag_params.get('description', ''),
        'email_on_failure': dag_params.get('email_on_failure', False),
        'email_on_success': dag_params.get('email_on_success', False),
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
        extract_kwargs = {
            'url': dag_params.get('url'),
            'output_filename': f'{dag_id}_extract_to_transform_{{{{ ts }}}}.{file_extension}',
            'logical_timestamp': f'{{{{ ts }}}}',
            **config,
            **dag_params.get('tasks').get('extract').get('kwargs', {}),
        }
        extract_task = PythonOperator(
            task_id='extract',
            python_callable=functions.extract,
            retries=dag_params.get('tasks').get('extract').get('retries', 1),
            retry_delay=dag_params.get('tasks').get('extract').get('retry_delay', 5),
            op_kwargs=extract_kwargs,
        )

        transform_kwargs = {
            'input_filename': f'{dag_id}_extract_to_transform_{{{{ ts }}}}.{file_extension}',
            'output_filename': f'{dag_id}_transform_to_load_{{{{ ts }}}}.csv',
            **config,
            **dag_params.get('tasks').get('transform').get('kwargs', {}),
        }
        transform_task = PythonOperator(
            task_id='transform',
            python_callable=functions.transform,
            retries=dag_params.get('tasks').get('transform').get('retries', 1),
            retry_delay=dag_params.get('tasks').get('transform').get('retry_delay', 5),
            op_kwargs=transform_kwargs,
        )

        extract_task >> transform_task

    return dag
