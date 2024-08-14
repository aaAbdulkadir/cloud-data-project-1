import os
import polars as pl
from polars.testing import assert_frame_equal

from new_york_taxi_functions import extract, transform, extract_historical
from dag_generator import (
    load_yml_file,
    load_json_file
)
import pendulum
from zipfile import ZipFile, is_zipfile

from test_base import TestBase


class Test(TestBase):
    def setUp(self):
        self.base_dir = os.path.dirname(__file__)
        self.yml_path = os.path.join(self.base_dir, 'new_york_taxi.yml')
        self.yml = load_yml_file(self.yml_path)
        self.url = self.yml['new_york_taxi']['url']
        self.logical_timestamp = pendulum.datetime(2024, 1, 1)
        self.historical_logical_timestamp = pendulum.datetime(2023, 1, 1)
        self.config_path = os.path.join(self.base_dir, 'config/config.json')
        self.config = load_json_file(self.config_path)
        
        # yellow taxi
        self.extracted_yellow_filename = os.path.join(
            self.base_dir, 'test_data/test_file_yellow.parquet'
        )
        self.transformed_yellow_filename = os.path.join(
            self.base_dir, 'test_data/transformed_yellow.csv'
        )
        
        # green taxi
        self.extracted_green_filename = os.path.join(
            self.base_dir, 'test_data/test_file_green.parquet'
        )
        self.transformed_green_filename = os.path.join(
            self.base_dir, 'test_data/transformed_green.csv'
        )
        
        # historical
        self.extracted_green_historical_filename = os.path.join(
            self.base_dir, 'test_data/test_file_green_historical.zip'
        )
        self.transformed_green_historical_filename = os.path.join(
            self.base_dir, 'test_data/transformed_green_historical.csv'
        )
        
    def tearDown(self):
        if os.path.exists(self.output_filename):
            os.remove(self.output_filename)
        
        
    def test_extract_yellow(self):
        self.output_filename = 'test_extract.parquet'
        params = {'taxi_type': 'yellow'}
        
        extract(
            self.url,
            self.output_filename,
            self.logical_timestamp,
            params
        )
        
        df = pl.read_parquet(self.output_filename)
        
        assert set(list(df.columns)) == set(self.config['yellow_taxi']['expected_columns_extract'])
        assert df.shape[0] > 10

            
                
    def test_extract_green(self):
        self.output_filename = 'test_extract.parquet'
        params = {'taxi_type': 'green'}
        
        extract(
            self.url,
            self.output_filename,
            self.logical_timestamp,
            params
        )
        
        df = pl.read_parquet(self.output_filename)
        
        assert set(list(df.columns)) == set(self.config['green_taxi']['expected_columns_extract'])
        assert df.shape[0] > 10

                
        
    def test_extract_green_historical(self):
        self.output_filename = 'test_extract_historical.zip'
        params = {'taxi_type': 'green', 'start_date': '2023-01-01', 'end_date': '2023-01-01'}
        
        extract_historical(
            self.url,
            self.output_filename,
            params,
        )
        
        self.assertTrue(is_zipfile(self.output_filename), "Output is not a valid ZIP file")
        
        with ZipFile(self.output_filename, 'r') as zipf:
            file_list = zipf.namelist() 
            assert len(file_list) == 1, f"Expected 12 files in the ZIP, found {len(file_list)}"

            
            
    def test_transform_yellow(self):
        input_filename = self.extracted_yellow_filename
        self.output_filename = 'test_transform.csv'
        params = {'taxi_type': 'yellow', 'historical': False}
        
        transform(
            input_filename,
            self.output_filename,
            self.config,
            params
        )
        
        transformed_df = pl.read_csv(self.output_filename)
        expected_df = pl.read_csv(self.transformed_yellow_filename)
        
        assert_frame_equal(expected_df, transformed_df)

                
                
    def test_transform_green(self):
        input_filename = self.extracted_green_filename
        self.output_filename = 'test_transform.csv'
        params = {'taxi_type': 'green', 'historical': False}
        
        transform(
            input_filename,
            self.output_filename,
            self.config,
            params
        )
        
        transformed_df = pl.read_csv(self.output_filename)
        expected_df = pl.read_csv(self.transformed_green_filename)
        
        assert_frame_equal(expected_df, transformed_df)

                
    def test_transform_green_historical(self):
        input_filename = self.extracted_green_historical_filename
        self.output_filename = 'test_transform_historical.csv'
        params = {'taxi_type': 'green', 'historical': True}
        
        transform(
            input_filename,
            self.output_filename,
            self.config,
            params
        )
        
        transformed_df = pl.read_csv(self.output_filename)
        expected_df = pl.read_csv(self.transformed_green_historical_filename)
        
        assert_frame_equal(expected_df, transformed_df)

    
    def test_load_yellow(self):
        self.validate_load(
            self.yml_path,
            self.transformed_yellow_filename,
            'load_yellow'
        )
        
    def test_load_green(self):
        self.validate_load(
            self.yml_path,
            self.transformed_green_filename,
            'load_green'
        )
        
        