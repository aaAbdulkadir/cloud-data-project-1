import os
import sys
from pathlib import Path
import pendulum

# Add the project root to the Python path
project_root = Path(__file__).parents[1]
sys.path.append(str(project_root))

from dags.dag_generator import (
    load_yml_file,
    load_json_file,
    import_functions,
    create_dag,
    get_filename_template
)
from dags.test_base import TestBase

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
        self.dummy_dag_transform_filename = os.path.join(
            self.dummy_dag_dir, 'transform.csv'
        )
        
    def test_get_filename_template(self):
        dag_id = 'example_dag'
        task_id = 'task_1'
        next_task_id = 'task_2'
        ts = '2024-08-02T12:00:00Z'
        file_extension = 'csv'
        STAGING_DATA = '/home/ubuntu/airflow/staging_data'
        expected_output = f"{STAGING_DATA}/{dag_id}_{task_id}_to_{next_task_id}_{ts}.{file_extension}"
        
        result = get_filename_template(dag_id, task_id, next_task_id, ts, file_extension)
        
        assert result == expected_output

    def test_load_yaml_is_valid(self):
        self.validate_config(self.dummy_dag_yml)

    def test_load_dag_config(self):
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
        
    


    def test_create_dag(self):

        dag = create_dag(self.dummy_dag_yml)

        print('yes')