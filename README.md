Plan:

- AWS
    - Airflow on EC2
    - DAGs stored in s3 (for scalability/ not saving on server)
    - Staging data in s3 (scalability)
    - RDS or Redshift?

- CI/CD 
    - GitHub Actions
    - Push dags and dependencies to s3 bucket which will be read by airflow by setting up a connection
    - 

set up:
- extracted data is stored in s3 staging data, transformed file is stored in s3 as staging data, transformed file is loaded into some db, dynamically generate dags using yml file. The idea is to have a pipeline in place to provide datasets to stakeholders