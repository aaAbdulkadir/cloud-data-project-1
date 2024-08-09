import os
import polars as pl
from polars.testing import assert_frame_equal

from new_york_taxi_functions import extract, transform
from dag_generator import (
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
        
        
    def test_extract_yellow(self):
        output_filename = 'test_extract.parquet'
        params = {'taxi_type': 'yellow'}
        try:
            extract(
                self.url,
                output_filename,
                self.logical_timestamp,
                params
            )
            
            df = pl.read_parquet(output_filename)
            
            assert set(list(df.columns)) == set(self.config['yellow_taxi']['expected_columns_extract'])
            assert df.shape[0] > 10
            
        finally:
            if os.path.exists(output_filename):
                os.remove(output_filename)
                
                
    def test_extract_green(self):
        output_filename = 'test_extract.parquet'
        params = {'taxi_type': 'green'}
        try:
            extract(
                self.url,
                output_filename,
                self.logical_timestamp,
                params
            )
            
            df = pl.read_parquet(output_filename)
            
            assert set(list(df.columns)) == set(self.config['green_taxi']['expected_columns_extract'])
            assert df.shape[0] > 10
            
        finally:
            if os.path.exists(output_filename):
                os.remove(output_filename)
                
                
                
    def test_transform_yellow(self):
        input_filename = self.extracted_yellow_filename
        output_filename = 'test_transform.csv'
        params = {'taxi_type': 'yellow'}
        try:
            transform(
                input_filename,
                output_filename,
                self.config,
                params
            )
            
            transformed_df = pl.read_csv(output_filename)
            expected_df = pl.read_csv(self.transformed_yellow_filename)
            
            assert_frame_equal(expected_df, transformed_df)
           
        finally:
            if os.path.exists(output_filename):
                os.remove(output_filename)
                
                
    def test_transform_green(self):
        input_filename = self.extracted_green_filename
        output_filename = 'test_transform.csv'
        params = {'taxi_type': 'green'}
        try:
            transform(
                input_filename,
                output_filename,
                self.config,
                params
            )
            
            transformed_df = pl.read_csv(output_filename)
            expected_df = pl.read_csv(self.transformed_green_filename)
            
            assert_frame_equal(expected_df, transformed_df)
           
        finally:
            if os.path.exists(output_filename):
                os.remove(output_filename)
                
    
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
        
        