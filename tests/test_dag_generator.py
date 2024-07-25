import unittest
import os
import sys
from pathlib import Path

# Add the project root to the Python path
project_root = Path(__file__).parents[1]
sys.path.append(str(project_root))

from dags.dag_generator import (
    load_dag_config,
    load_json_file,
    import_functions,
    create_dag,
)


class TestDAGGenerator(unittest.TestCase):
    def setUp(self):
        self.base_dir = project_root
        self.dags_dir = os.path.join(self.base_dir, 'dags')
        self.dummy_dag_dir = os.path.join(
            self.dags_dir, 'dummy_dag'
        )
        self.dummy_dag_yml = os.path.join(
            self.dags_dir, self.dummy_dag_dir, 'dummy_dag.yml'
        )
        self.json_file_path = os.path.join(
            self.dags_dir, 'dummy_dag', 'config', 'config.json'
        )

    def test_load_dag_config(self):
        yml_file_path = self.dummy_dag_yml
        yml = load_dag_config(yml_file_path)
        self.assertIsNotNone(yml)

    def test_load_json_file(self):
        json_data = load_json_file(self.json_file_path)
        assert json_data['foo'] == 'bar'

    def test_import_functions(self):
        module = import_functions(self.dummy_dag_dir)
        self.assertIsNotNone(module)
        assert module.extract('http://example.com', 'output.csv') == 1
        assert module.transform('input.csv', 'output.csv') == 1

    def test_create_dag(self):

        create_dag(self.dummy_dag_dir)