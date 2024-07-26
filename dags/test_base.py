import unittest
from dags.dag_generator import load_yml_file

class TestBase(unittest.TestCase):

    def validate_config(self, yml_file_path):
        """Test if the YAML file has all required fields and valid dependencies."""
        
        required_fields = {
            'schedule_interval',
            'url',
            'python_callable_file',
            'tasks',
            'email'
        }
        task_required_fields = {
            'python_callable',
        }
        load_required_fields = {
            'mode',
            'dataset_name',
            'fields'
        }
        field_required_subfields = {
            'name',
            'type'
        }

        # Load the YAML configuration
        config = load_yml_file(yml_file_path)
        
        for dag_id, dag_params in config.items():
            # Check if all required top-level fields are present and not None or empty
            self.assertTrue(set(dag_params.keys()).issuperset(required_fields), f"Missing required fields in DAG {dag_id}")
            for field in required_fields:
                self.assertIsNotNone(dag_params.get(field), f"Field '{field}' in DAG {dag_id} must not be None")
                self.assertNotEqual(dag_params.get(field), '', f"Field '{field}' in DAG {dag_id} must not be empty")

            # Gather all task IDs for dependency validation
            task_ids = set(dag_params.get('tasks', {}).keys())

            tasks = dag_params.get('tasks', {})
            for task_id, task_params in tasks.items():
                # Validate non-load tasks for required fields
                if task_id != 'load':
                    self.assertTrue(set(task_params.keys()).issuperset(task_required_fields), f"Missing required 'python_callable' field in task {task_id} of DAG {dag_id}")
                    self.assertIsNotNone(task_params.get('python_callable'), f"Field 'python_callable' in task {task_id} of DAG {dag_id} must not be None")
                    self.assertNotEqual(task_params.get('python_callable'), '', f"Field 'python_callable' in task {task_id} of DAG {dag_id} must not be empty")
                else:
                    # Validate load tasks for required fields
                    self.assertTrue(set(task_params.keys()).issuperset(load_required_fields), f"Missing required fields in 'load' task of DAG {dag_id}")
                    for field in load_required_fields:
                        self.assertIsNotNone(task_params.get(field), f"Field '{field}' in 'load' task of DAG {dag_id} must not be None")
                        self.assertNotEqual(task_params.get(field), '', f"Field '{field}' in 'load' task of DAG {dag_id} must not be empty")

                    # Validate that each field in 'fields' has 'name' and 'type'
                    fields = task_params.get('fields', [])
                    for index, field in enumerate(fields):
                        self.assertTrue(set(field.keys()).issuperset(field_required_subfields), f"Field {index} in 'fields' of 'load' task in DAG {dag_id} is missing 'name' or 'type'")
                        self.assertIsNotNone(field.get('name'), f"Field 'name' in field {index} of 'load' task in DAG {dag_id} must not be None")
                        self.assertNotEqual(field.get('name'), '', f"Field 'name' in field {index} of 'load' task in DAG {dag_id} must not be empty")
                        self.assertIsNotNone(field.get('type'), f"Field 'type' in field {index} of 'load' task in DAG {dag_id} must not be None")
                        self.assertNotEqual(field.get('type'), '', f"Field 'type' in field {index} of 'load' task in DAG {dag_id} must not be empty")

                # Validate that all dependencies are defined
                if task_id != 'extract':
                    dependencies = task_params.get('dependencies', [])
                    if dependencies:
                        missing_dependencies = [dep for dep in dependencies if dep not in task_ids]
                        self.assertTrue(not missing_dependencies, f"Task {task_id} in DAG {dag_id} has undefined dependencies: {missing_dependencies}")
                    else:
                        self.assertFalse(True, f"Task {task_id} in DAG {dag_id} has no dependencies")


    def validate_load(self, yml_file_path):
        """Placeholder for load function validation."""
        # Implement the load function validation as per your specific requirements
        pass

