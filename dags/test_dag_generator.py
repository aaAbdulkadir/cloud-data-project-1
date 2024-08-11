import os
import sys
from pathlib import Path
import pendulum
from airflow.models import DagBag
from datetime import timedelta
from airflow.operators.python_operator import PythonOperator
import mock

# Add the project root to the Python path
project_root = Path(__file__).parents[1]
sys.path.append(str(project_root))

from dag_generator import (
    load_yml_file,
    load_json_file,
    import_functions,
    create_dag,
    get_filename_template,
    load_config_if_exists
)
from test_base import TestBase

class TestDAGGenerator(TestBase):
    def setUp(self):
        self.base_dir = project_root
        self.dags_dir = os.path.join(self.base_dir, 'dags')
        self.dummy_dag_dir = os.path.join(
            self.dags_dir, 'dummy_dag'
        )
        self.dummy_dag_yml = os.path.join(
            self.dummy_dag_dir, 'dummy_dag.yml'
        )
        self.json_file_path = os.path.join(
             self.dummy_dag_dir, 'config', 'config.json'
        )
        self.dummy_dag_functions_path = os.path.join(
            self.dummy_dag_dir, 'dummy_dag_functions.py'
        )
        self.logical_timestamp = pendulum.now()

        self.dummy_dag_extract_filename = os.path.join(
            self.dummy_dag_dir, 'extract.csv'
        )
        self.dummy_dag_config = os.path.join(
            self.dummy_dag_dir, 'config/config.json'
        )
        self.test_dummy_transform_filename = os.path.join(
            self.dummy_dag_dir, "test_data/test_transform_file.csv"
        )
        
    def test_get_filename_template(self):
        dag_id = 'example_dag'
        task_id = 'task_1'
        next_task_id = 'task_2'
        ts = '2024-08-02T12:00:00Z'
        file_extension = 'csv'
        expected_output = f"{dag_id}/{dag_id}_{task_id}_to_{next_task_id}_{ts}.{file_extension}"
        
        result = get_filename_template(dag_id, task_id, next_task_id, ts, file_extension)
        
        assert result == expected_output

    def test_load_yaml_is_valid(self):
        self.validate_config(self.dummy_dag_yml)

    def test_load_yml_file(self):
        yml_file_path = self.dummy_dag_yml
        yml = load_yml_file(yml_file_path)
        self.assertIsNotNone(yml)

    def test_load_json_file(self):
        json_data = load_json_file(self.json_file_path)
        assert json_data['foo'] == 'bar'

    def test_import_functions(self):
        config = {'foo': 'bar'}
        module = import_functions(self.dummy_dag_functions_path)
        self.assertIsNotNone(module)
        
        module.extract(
            'http://example.com',
            self.dummy_dag_extract_filename,
            self.logical_timestamp,
            config,
        )
        self.assertTrue(os.path.isfile(self.dummy_dag_extract_filename))
        
        os.remove(self.dummy_dag_extract_filename)
        
    def test_load_config_if_exists(self):
        dag_parmas = {'config_path': 'config/config.json'}

        config = load_config_if_exists(self.dummy_dag_dir, dag_parmas)
        
        assert config['foo'] == 'bar'
    
        
    @mock.patch('airflow.models.Variable.get', return_value='mock-s3-bucket')
    def test_create_dag(self, mock_variable):
        
        # Create the DAG using the provided create_dag function
        dag = create_dag(self.dummy_dag_yml)
        dag_bag = DagBag(dag_folder=self.dags_dir, include_examples=False)
        
        # Ensure the DAG was loaded successfully
        self.assertIsNotNone(dag)
        self.assertFalse(dag_bag.import_errors)

        # Load the YAML configuration for comparison
        dag_config = load_yml_file(self.dummy_dag_yml)['dummy_dag']

        # Test DAG properties
        self.assertEqual(dag.schedule_interval, dag_config['schedule_interval'])
        self.assertEqual(dag.description, dag_config.get('description'))

        # Test default args
        default_args = dag.default_args
        self.assertEqual(default_args['owner'], dag_config.get('owner'))
        self.assertEqual(default_args['email'], [dag_config['email']])
        self.assertEqual(default_args['email_on_failure'], dag_config.get('email_on_failure'))
        self.assertEqual(default_args['email_on_retry'], dag_config.get('email_on_retry'))

        # Test tasks
        tasks = dag_config['tasks']
        for task_id, task_config in tasks.items():
            task = dag.get_task(task_id)
            self.assertIsNotNone(task)
            self.assertIsInstance(task, PythonOperator)

        # Test task dependencies
        for task_id, task_config in tasks.items():
            task = dag.get_task(task_id)
            for dependency in task_config.get('dependencies', []):
                upstream_task = dag.get_task(dependency)
                self.assertIn(upstream_task, task.upstream_list)

        # Verify the number of tasks
        self.assertEqual(len(dag.tasks), len(tasks))

        # Optionally, test specific task configurations
        extract_task = dag.get_task('extract')
        self.assertEqual(extract_task.op_kwargs['url'], dag_config['url'])

        load_task = dag.get_task('load')
        self.assertEqual(load_task.op_kwargs['dataset_name'], tasks['load']['dataset_name'])
        self.assertEqual(load_task.op_kwargs['mode'], tasks['load']['mode'])
        self.assertEqual(load_task.op_kwargs['fields'], tasks['load']['fields'])
        
    def test_load_to_sqlite(self):
        self.validate_load(self.dummy_dag_yml, self.test_dummy_transform_filename)