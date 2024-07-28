import unittest
from dags.dag_generator import load_yml_file
import os
class TestBase(unittest.TestCase):

    def validate_config(self, yml_file_path):
        """Test if the YAML file has all required fields and valid dependencies."""
        
        # Load the YAML configuration
        dag_id = os.path.basename(yml_file_path).split('.')[0]
        dag_params = load_yml_file(yml_file_path)[dag_id]

        # Check top-level required fields
        required_fields = ['schedule_interval', 'url', 'email', 'python_callable_file', 'tasks']
        for field in required_fields:
            self.assertIn(field, dag_params, f"Missing required field: {field}")

        # Check tasks
        tasks = dag_params['tasks']
        self.assertIn('extract', tasks, "Missing 'extract' task")
        self.assertIn('transform', tasks, "Missing 'transform' task")
        self.assertIn('load', tasks, "Missing 'load' task")

        # Check extract and transform tasks
        for task in ['extract', 'transform']:
            self.assertIn('python_callable', tasks[task], f"Missing 'python_callable' in {task} task")

        # Check load task
        load_task = tasks['load']
        load_required_fields = ['mode', 'dataset_name', 'fields']
        for field in load_required_fields:
            self.assertIn(field, load_task, f"Missing required field in load task: {field}")

        # Check fields in load task
        fields = load_task['fields']
        self.assertIsInstance(fields, list, "'fields' should be a list")
        for field in fields:
            self.assertIn('name', field, "Missing 'name' in field")
            self.assertIn('type', field, "Missing 'type' in field")
            self.assertIsInstance(field['type'], str, "Field 'type' should be a string")
            ### test the field dtype is a postgres valid dtype
            # self.assertTrue(self.is_valid_postgres_type(field['type']), f"Invalid Postgres type: {field['type']}")


    def validate_load(self, yml_file_path):
        """Placeholder for load function validation."""
        # Implement the load function validation as per your specific requirements
        pass

