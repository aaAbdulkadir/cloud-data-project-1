# Understanding the Code Base

The code base provides a flexible framework for dynamically generating Apache Airflow DAGs based on YAML configuration files. Below, we break down the various components and standards for utilizing this system effectively.

## Required Files

1. **Functions File**: This file should include the core business logic for the `extract`, `transform`, and `load` operations. These functions can be named anything, but their names must be specified in the YAML file under the `python_callable` field.

2. **Helper Functions File**: This is used to refactor common code into reusable functions, making the main codebase cleaner and more maintainable.

3. **YAML File for DAG Definition**: The YAML file outlines the structure and configuration of each DAG. It specifies tasks, dependencies, scheduling, and other parameters.

4. **Component Test File**: This file contains tests for the `extract`, `transform`, and `load` functions, ensuring they perform as expected.

5. **Unit Test File**: This file is used to test the helper functions in isolation, typically involving mocking to avoid live calls.

6. **README**: This document provides an overview of the DAG, explaining its purpose, configuration options, and how to use it.

7. **Test Data Folder**: This folder stores sample files used for testing purposes.

8. **Config Folder**: This folder contains any additional parameters or configuration files required for the scraping process, aiding in keeping the code clean and manageable.

## Dynamic DAGs

The system is designed to dynamically generate DAGs based on configurations specified in YAML files. This approach provides flexibility and scalability by defining Airflow parameters in a configuration-driven manner.

- **Configuration**: The YAML file must include several key variables:
  - `schedule_interval`: Defines how often the DAG should run.
  - `url`: Specifies the source URL for data extraction.
  - `email`: Used for notifications.
  - `python_callable_file`: The path to the file containing the functions for the DAG.
  - `tasks`: Lists all tasks, specifying their roles and dependencies.

- **Task Naming**: The YAML file should include tasks named with keywords such as 'extract' and 'load'. These tasks can be renamed but should retain these keywords to ensure proper functioning. Tasks named 'transform' can use any name, but it's a best practice to follow the convention of naming them sequentially as `extract`, `transform`, and `load`.

- **Task Dependencies**: You can define as many tasks as needed and set up dependencies according to your workflow. The system expects that the `extract` task should come first and the `load` task should be last.

## Functions

The main functions used within the DAGs include:

- **Extract**: Requires parameters such as `output_filename`, `url`, and optionally `logical_timestamp`, `config`, and `params`.

- **Transform**: Needs `input_filename`, `output_filename`, and may also take `config` and `params`.

- **Load**: Requires `input_filename`, `dataset_name`, `mode`, and `fields`.

## Task Wrapper

The `task_wrapper` function serves as a central mechanism for managing task execution, file handling, and data transfer. Here's how it operates:

1. **File Handling**:
   - **Extract Task**: Saves the output file locally and uploads it to an S3 staging bucket. After uploading, the local file is removed.
   - **Transform Task**: Retrieves the file from S3, processes it, saves the result to a new file, and uploads it back to S3. The local file is deleted after processing.
   - **Load Task**: Downloads the transformed file from S3, loads it into a PostgreSQL database, and then removes the local file.

2. **XCom Integration**: The `task_wrapper` uses Airflow's XCom to pass file names between tasks, ensuring that each task receives the correct input and output files.

3. **Latest File Comparison Check**: If specified in the YAML file, the wrapper performs a comparison between the newly extracted file and the latest file in the S3 bucket. This helps avoid processing duplicate or unchanged files by comparing their content and names.

By following these standards and utilizing the provided functions and wrappers, you can efficiently create, manage, and execute dynamic DAGs in Airflow. This setup not only enhances flexibility but also ensures that the data processing pipeline is robust and adaptable to various needs.

## Adding New DAGs

To add a new DAG, follow these steps:

1. Create a new directory in the `dags` folder. The name of this directory should match the name of the new DAG.

2. Inside the new directory, create a YAML file for the DAG configuration. The name of the YAML file should be the same as the directory name.

3. The first line of the YAML file should specify the name of the DAG, matching the directory and YAML file name.

You can find examples of YAML configurations and DAG definitions in the `dags` folder, under each scrape name. For instance, you can refer to the [New York Taxi Scrape](https://github.com/aaAbdulkadir/cloud-data-project-1/tree/main/dags/new_york_taxi) folder and the [Dummy DAG](.https://github.com/aaAbdulkadir/cloud-data-project-1/tree/main/dags/dummy_dag) folder for sample configurations and DAG definitions.
