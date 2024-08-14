import os
import polars as pl
import pendulum

from new_york_taxi_helper_functions import (
    formulate_url,
    get_response_data,
    join_taxi_zone_data
)
import mock
import pytest 
from airflow.exceptions import AirflowSkipException, AirflowException

from polars.testing import assert_frame_equal
from test_base import TestBase

class Test(TestBase):
    def setUp(self):
        self.base_dir = os.path.dirname(__file__)
        
    def test_formulate_url(self):
        url_template = "https://example.com/{taxi_type}/{year}/{month}/data"
        taxi_type = "yellow"
        logical_timestamp = pendulum.datetime(2024, 8, 12)

        expected_url = "https://example.com/yellow/2024/08/data"
        assert formulate_url(url_template, taxi_type, logical_timestamp) == expected_url
        
    
    @mock.patch('requests.get')
    def test_get_response_data_success(self, mock_get):
        url = "http://example.com/success"
        expected_content = b"Success"
        
        mock_get.return_value.status_code = 200
        mock_get.return_value.content = expected_content
        
        result = get_response_data(url)
        
        assert result == expected_content
                
        
    @mock.patch('requests.get')
    def test_get_response_data_status_403(self, mock_get):
        url = "http://example.com/forbidden"
        
        mock_get.return_value.status_code = 403
        
        msg = (
            "Failed to fetch data from http://example.com/forbidden. "
            "Status code: 403. URL may not be available yet, skipping."
        )
        with pytest.raises(AirflowSkipException, match=msg):
            get_response_data(url)
            
            
    @mock.patch('requests.get')
    def test_get_response_data_status_other(self, mock_get):
        """Test that get_response_data raises AirflowException for other status codes."""
        url = "http://example.com/error"
        
        mock_get.return_value.status_code = 500
        
        msg = (
            "Failed to fetch data from http://example.com/error. "
            "Status code: 500"
        )
            
        with pytest.raises(AirflowException, match=msg):
            get_response_data(url)
        
        
        
    def test_join_taxi_zone_data(self):
        df = pl.DataFrame({
            'pickup_location_id': pl.Series([1, 2, 3], dtype=pl.Int32),
            'dropoff_location_id': pl.Series([4, 5, 6], dtype=pl.Int32)
        })

        taxi_zone_lookup_df = pl.DataFrame({
            'location_id': pl.Series([1, 2, 4, 5], dtype=pl.Int32),
            'zone_name': ['Zone A', 'Zone B', 'Zone C', 'Zone D']
        })

        result_df = join_taxi_zone_data(df, taxi_zone_lookup_df)

        expected_df = pl.DataFrame({
            'zone_name': ['Zone C', 'Zone D', None] 
        })


        assert_frame_equal(result_df, expected_df)