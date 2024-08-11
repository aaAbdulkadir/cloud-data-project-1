# Main

This project is an ETL pipeline built using Apache Airflow, designed to efficiently manage and automate data scraping tasks from multiple sources. The infrastructure is deployed on an EC2 instance with S3 buckets for storing DAGs (Directed Acyclic Graphs) and staging data, and an RDS (Relational Database Service) for storing the final datasets.

The core of the project is a dynamic framework that allows for the easy creation of multiple scrapes (DAGs) based on a YAML configuration file. This setup streamlines the process of extracting, transforming, and loading data into the RDS. The DAGs and their dependencies are version-controlled and deployed through a CI/CD pipeline, ensuring that the S3 bucket and the Airflow instance remain in sync. This infrastructure provides a scalable solution for writing and deploying custom data scraping tasks, enabling efficient data management and processing.



## Part 1: Cloud Infrastructure Set-Up


In this section, we establish the foundational cloud infrastructure required to run our ETL pipeline using AWS services. The key components include:

### 1. Identity and Access Management (IAM):

User and Group Setup: We create a dedicated IAM user (data-pipeline) and an IAM group (data-pipeline-group) to manage permissions securely.
Policy Creation: A custom IAM policy is defined to provide the necessary permissions to access S3 buckets, EC2 instances, and RDS databases. This ensures that the user has only the minimum permissions required for the project.

### 2. Amazon S3:

Bucket Creation: Two S3 buckets are created—one for storing DAGs (data-pipeline-airflow-dags) and another for staging data (data-pipeline-staging-data).

### 3. Amazon RDS:

Database Setup: An RDS PostgreSQL database (data-pipeline-database) is created to store the final datasets. The database is configured with the free-tier settings, and appropriate security groups are set up to allow access from the EC2 instance.

### 4. Amazon EC2:

Instance Launch: An EC2 instance (airflow-server) is launched with Ubuntu as the operating system. This instance hosts the Airflow server, which manages and executes the ETL tasks.
Elastic IP: An Elastic IP is allocated and associated with the EC2 instance to ensure a static IP address for accessing the Airflow server.

### 5. Airflow Installation:

Environment Setup: The EC2 instance is configured with Python, and Airflow is installed in a virtual environment. The Airflow web server and scheduler are set up to start automatically upon booting the EC2 instance, ensuring continuous operation.

### 6. AWS CLI Installation:

The AWS CLI is installed on the EC2 instance to facilitate interaction with other AWS services, such as S3 and RDS.

### 7. IAM Credentials Configuration on EC2:

IAM credentials are configured on the EC2 instance to allow it to interact with AWS services securely.

### 8. CI/CD Pipeline Setup:

A GitHub Actions pipeline is configured to automate the deployment of DAGs and their dependencies. This ensures that any changes pushed to the repository are automatically reflected in the Airflow instance, maintaining synchronization between the S3 bucket and the EC2 server.
This setup lays the groundwork for the subsequent parts of the project, enabling a robust and scalable ETL pipeline using Airflow on AWS.

For detailed instructions on setting up the cloud infrastructure, please refer to the [AWS Service Setup](https://github.com/aaAbdulkadir/cloud-data-project-1/blob/main/notes/cloud_setup.md) notes.




## Part 3: Developing Airflow Infrastructure/Codebase
