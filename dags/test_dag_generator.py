import os
import sys
from pathlib import Path
import pendulum
from airflow.models import DagBag
from datetime import timedelta
from airflow.operators.python_operator import PythonOperator
import mock
import pytest
import polars as pl
from airflow.exceptions import AirflowException, AirflowSkipException
# Add the project root to the Python path
project_root = Path(__file__).parents[1]
sys.path.append(str(project_root))

from dag_generator import (
    load_yml_file,
    load_json_file,
    import_functions,
    create_dag,
    get_filename_template,
    load_config_if_exists,
    read_in_data,
    check_two_dataframes
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
        ts = '2024-08-02T12:00:00Z'
        file_extension = 'csv'
        expected_output = f"{dag_id}/{dag_id}_{task_id}_{ts}.{file_extension}"
        
        result = get_filename_template(dag_id, task_id, ts, file_extension)
        
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
        params = {'mode': 'delta'}
        module = import_functions(self.dummy_dag_functions_path)
        self.assertIsNotNone(module)
        
        module.extract(
            'http://example.com',
            self.dummy_dag_extract_filename,
            self.logical_timestamp,
            config,
            params
        )
        self.assertTrue(os.path.isfile(self.dummy_dag_extract_filename))
        
        os.remove(self.dummy_dag_extract_filename)
        
    def test_load_config_if_exists(self):
        dag_parmas = {'config_path': 'config/config.json'}

        config = load_config_if_exists(self.dummy_dag_dir, dag_parmas)
        
        assert config['foo'] == 'bar'
        
        
    def test_check_two_dataframes_identical(self):
        df_1 = pl.DataFrame({"col1": [1, 2, 3], "col2": [4, 5, 6]})
        df_2 = pl.DataFrame({"col2": [4, 5, 6], "col1": [1, 2, 3]})

        with pytest.raises(AirflowSkipException):
            check_two_dataframes(df_1, df_2)

    def test_check_two_dataframes_different(self):
        df_1 = pl.DataFrame({"col1": [1, 2, 3], "col2": [4, 5, 6]})
        df_2 = pl.DataFrame({"col1": [1, 2, 3], "col2": [7, 8, 9]})

        assert check_two_dataframes(df_1, df_2) == True

    def test_check_two_dataframes_column_difference(self):
        df_1 = pl.DataFrame({"col1": [1, 2, 3], "col2": [4, 5, 6]})
        df_2 = pl.DataFrame({"col1": [1, 2, 3], "col3": [4, 5, 6]})

        with pytest.raises(AirflowException):
            check_two_dataframes(df_1, df_2)

    def test_read_in_data_supported_formats(self):
        test_cases = [
            ("data.csv", "csv"),
            ("data.xlsx", "xlsx"),
            ("data.xls", "xls"),
            ("data.parquet", "parquet"),
        ]
        
        for filename, expected_extension in test_cases:
            with mock.patch("dag_generator.pl.read_csv", return_value=pl.DataFrame({"col1": [1, 2, 3]})), \
                 mock.patch("dag_generator.pl.read_excel", return_value=pl.DataFrame({"col1": [1, 2, 3]})), \
                 mock.patch("dag_generator.pl.read_parquet", return_value=pl.DataFrame({"col1": [1, 2, 3]})):

                df = read_in_data(filename)
                self.assertIsInstance(df, pl.DataFrame)

    def test_read_in_data_unsupported_format(self):
        with pytest.raises(AirflowException):
            read_in_data("data.unsupported")
    
        
    @mock.patch('airflow.models.Variable.get', return_value='mock-s3-bucket')
    def test_create_dag(self, mock_variable):
        
        # Create the DAG using the provided create_dag function
        dag = create_dag(self.dummy_dag_yml)
        dag_bag = DagBag(dag_folder=self.dags_dir, include_examples=False)
        
        self.assertIsNotNone(dag)
        self.assertFalse(dag_bag.import_errors)

        dag_config = load_yml_file(self.dummy_dag_yml)['dummy_dag']

        self.assertEqual(dag.schedule_interval, dag_config['schedule_interval'])
        self.assertEqual(dag.description, dag_config.get('description'))

        default_args = dag.default_args
        self.assertEqual(default_args['owner'], dag_config.get('owner'))
        self.assertEqual(default_args['email'], [dag_config['email']])
        self.assertEqual(default_args['email_on_failure'], dag_config.get('email_on_failure'))
        self.assertEqual(default_args['email_on_retry'], dag_config.get('email_on_retry'))

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

        self.assertEqual(len(dag.tasks), len(tasks))

        extract_task = dag.get_task('extract')
        self.assertEqual(extract_task.op_kwargs['url'], dag_config['url'])

        load_task = dag.get_task('load')
        self.assertEqual(load_task.op_kwargs['dataset_name'], tasks['load']['dataset_name'])
        self.assertEqual(load_task.op_kwargs['mode'], tasks['load']['mode'])
        self.assertEqual(load_task.op_kwargs['fields'], tasks['load']['fields'])
        
    def test_load_to_sqlite(self):
        self.validate_load(self.dummy_dag_yml, self.test_dummy_transform_filename)