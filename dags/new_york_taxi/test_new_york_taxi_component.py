import os

from new_york_taxi_functions import extract, transform
from ..dag_generator import (
    load_yml_file,
    load_json_file
)
import yaml
import pendulum

from test_base import TestBase


class Test(TestBase):
    def setUp(self):
        self.base_dir = os.path.dirname(__file__)
        self.yml_path = os.path.join(self.base_dir, 'new_york_taxi.yml')
        self.yml = load_yml_file(self.yml_path)
        self.url = self.yml['new_york_taxi']['url']
        self.logical_timestamp = pendulum.datetime(2024, 1, 1)
        self.config_path = os.path.join(self.base_dir, 'config/config.json')
        self.config = load_json_file(self.config_path)
        

        
    def test_extract(self):
        output_filename = 'test_extract.csv'
        try:
            extract(
                self.url,
                output_filename,
                self.logical_timestamp,
                self.config
            )
        finally:
            if os.path.exists(output_filename):
                os.remove(output_filename)
        
        