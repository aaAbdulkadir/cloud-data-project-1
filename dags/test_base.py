import unittest
from dag_generator import load_yml_file
import os
import pandas as pd
import sqlite3
import logging
from dag_generator import load_yml_file
from datetime import datetime
from dateutil.parser import parse as date_parse

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


    def validate_load(self, yml_file_path, input_filename: str, load_task_name: str):
        """Validate load function with an SQLite in-memory database."""
        logger = logging.getLogger('load_to_sqlite')
        
        # Load in yml file load variables
        dag_id = os.path.basename(yml_file_path).split('.')[0]
        load_params = load_yml_file(yml_file_path)[dag_id]['tasks'][load_task_name]
        mode = load_params['mode']
        dataset_name = load_params['dataset_name']
        fields = load_params['fields']
        upsert_key_fields = load_params.get('upsert_key_fields', [])  # Get upsert_key_fields if present
        
        df = pd.read_csv(input_filename)
        # order column names
        df = df[[field['name'] for field in fields]]
        connection = sqlite3.connect(':memory:')  # In-memory SQLite database
        
        try:
            cursor = connection.cursor()

            if mode == 'append':
                create_table_query = f"CREATE TABLE IF NOT EXISTS {dataset_name} ("
            elif mode == 'replace':
                drop_table_query = f"DROP TABLE IF EXISTS {dataset_name};"
                cursor.execute(drop_table_query)
                connection.commit()
                create_table_query = f"CREATE TABLE {dataset_name} ("
            elif mode == 'upsert':
                if not upsert_key_fields:
                    raise ValueError("upsert_key_fields must be provided for upsert mode.")
                create_table_query = f"CREATE TABLE IF NOT EXISTS {dataset_name} ("
            else:
                raise ValueError("Invalid mode. Choose 'append', 'replace', or 'upsert'.")

            for field in fields:
                field_name = field['name']
                field_type = field['type']
                create_table_query += f"{field_name} {field_type}, "
            create_table_query += "scraping_execution_date TIMESTAMP);"
            
            logger.info(f'Running {mode} on query: {create_table_query}')

            cursor.execute(create_table_query)
            connection.commit()

            columns = ', '.join([field['name'] for field in fields])
            columns += ", scraping_execution_date"
            placeholders = ', '.join(['?' for _ in range(len(fields) + 1)])

            if mode == 'upsert':
                # Create a temporary table for the new data
                temp_table_name = f"temp_{dataset_name}"
                cursor.execute(f"CREATE TEMPORARY TABLE {temp_table_name} AS SELECT * FROM {dataset_name} WHERE 0")

                # Insert data into the temporary table
                insert_query = f"INSERT INTO {temp_table_name} ({columns}) VALUES ({placeholders})"
                records = [(tuple(row) + (datetime.now(),)) for row in df.to_numpy()]
                cursor.executemany(insert_query, records)

                # Perform the upsert operation
                upsert_query = f"""
                    INSERT OR REPLACE INTO {dataset_name} ({columns})
                    SELECT {columns} FROM {temp_table_name}
                """
                cursor.execute(upsert_query)
                cursor.execute(f"DROP TABLE {temp_table_name}")

            else:
                insert_query = f"INSERT INTO {dataset_name} ({columns}) VALUES ({placeholders})"
                records = [(tuple(row) + (datetime.now(),)) for row in df.to_numpy()]
                cursor.executemany(insert_query, records)

            connection.commit()
            
            self.validate_constraints(df, fields)

        except Exception as e:
            raise Exception(f"Error loading data to SQLite: {str(e)}")

        finally:
            cursor.close()
            connection.close()
            logger.info('SQLite in-memory database instance destroyed.')

        logger.info('Loaded to database successfully.')
        
    def validate_constraints(self, df, fields):
        """Validate that the data matches the constraints specified in the fields."""
        for field in fields:
            field_name = field['name']
            field_type = field['type']
            
            if 'VARCHAR' in field_type:
                max_length = int(field_type.strip('VARCHAR()'))
                self.verify_varchar_length(df, field_name, max_length)
            elif field_type == 'INTEGER':
                self.verify_integer_type(df, field_name)
            elif field_type == 'FLOAT':
                self.verify_float_type(df, field_name)
            elif field_type == 'DATE':
                self.verify_date_type(df, field_name)
            elif field_type == 'TIMESTAMP':
                self.verify_datetime_type(df, field_name)
            elif field_type == 'NUMERIC':
                self.verify_numeric_type(df, field_name)
            elif field_type == 'BIGINT':
                self.verify_bigint_type(df, field_name)
            else:
                raise ValueError(f"Unsupported data type '{field_type}' for field '{field_name}'.")
            # Add more type checks if needed

    def verify_varchar_length(self, df, field_name, max_length):
        """Verify VARCHAR field length constraints."""
        for value in df[field_name]:
            if pd.notna(value) and len(str(value)) > max_length:
                raise ValueError(f"Value '{value}' exceeds maximum length of {max_length} for field '{field_name}'.")

    def verify_integer_type(self, df, field_name):
        """Verify INTEGER type constraints."""
        for value in df[field_name]:
            if pd.notna(value) and not isinstance(value, int):
                raise ValueError(f"Value '{value}' is not an integer for field '{field_name}'.")

    def verify_float_type(self, df, field_name):
        """Verify FLOAT type constraints."""
        for value in df[field_name]:
            if pd.notna(value):
                try:
                    float(value)
                except ValueError:
                    raise ValueError(f"Value '{value}' is not a float for field '{field_name}'.")

    def verify_numeric_type(self, df, field_name):
        """Verify NUMERIC type constraints."""
        for value in df[field_name]:
            if pd.notna(value):
                try:
                    float(value)  # NUMERIC can include integer and float
                except ValueError:
                    raise ValueError(f"Value '{value}' is not a numeric value for field '{field_name}'.")

    def verify_bigint_type(self, df, field_name):
        """Verify BIGINT type constraints."""
        for value in df[field_name]:
            if pd.notna(value):
                try:
                    int_value = int(value)
                    if not -2**63 <= int_value <= 2**63 - 1:
                        raise ValueError(f"Value '{value}' is out of BIGINT range for field '{field_name}'.")
                except ValueError:
                    raise ValueError(f"Value '{value}' is not a valid BIGINT for field '{field_name}'.")

    def verify_date_type(self, df, field_name):
        """Verify DATE type constraints."""
        for value in df[field_name]:
            if pd.notna(value):
                try:
                    date_parse(value).date()
                except (ValueError, TypeError):
                    raise ValueError(f"Value '{value}' is not a valid date for field '{field_name}'.")

    def verify_datetime_type(self, df, field_name):
        """Verify DATETIME type constraints."""
        for value in df[field_name]:
            if pd.notna(value):
                try:
                    date_parse(value)
                except (ValueError, TypeError):
                    raise ValueError(f"Value '{value}' is not a valid datetime for field '{field_name}'.")