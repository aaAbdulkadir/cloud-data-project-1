# new_york_taxi

## Overview

This scrape consists of scraping new york taxi data from the website: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

There are two DAGs:
- new_york_taxi
- new_york_taxi_historical

### Extract

**Delta:**

The extract works by using the logical timestamp to get the current year and month and input those values into the url as the url is year and month bound. Additionally, the url takes the value taxi_type, where there are multiple types but for this scrape, yellow and green taxi data are scraped. With these parameters, a request is made to the url and the contents are downloaded to parquet.

**Historical:**

The historical dag works the same way as the delta dag but in the yml file, the start and end dates are input to give a range of the dates tthat are required for the historical extraction. A zip file is created and a request for each month year within the year is made and saved to hhe zip as a list of parquet files.


### Transform

The transform works by reading in the parquet file to a polars dataframe (as the yellow taxi data is quite large and can exceed 3 million rows), remapping the column names, joining the zone look up table to the dataframe and filtering for required columns.

### Load

The data is loaded into hte postgres db.

### Interval

The scheduel runs at the end of each month as it sometimes takes time for the data to load. Writing this in 2024-08-15, it has been observed that the months June, July and August have not yet been uploaded, so if the dag does not find these dates, it will skip, but the dag can be triggered to run again to run on any date which will allow for data to be extracted when the source publishes the data.



## Possible Failures and Resolutions

### Data Recovery

- If the data is lost, there are two ways to recover it. The first way is to run the historical dag between a start and end date and this should retrieve all the data. If a particular date is lost, then the delta dag can be re-run by triggering config for that particular year and month, which will extract data for that date. The delta dag can also be used to get historical data due to the logical timestamp paramter.

- As both dags run on append mode, re running the same logical timestamp can cause duplicate data in the database. 
