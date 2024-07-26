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
        assert module.extract(
            'http://example.com',
            'output.csv',
            self.logical_timestamp,
            config
        ) == 1
        assert module.transform('input.csv', 'output.csv') == 1

    def test_create_dag(self):

        create_dag(self.dummy_dag_dir)