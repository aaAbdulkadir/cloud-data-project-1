# AWS Services Setup

**Note**: Everything is being done with the free tier of AWS.

## Setting up IAM

To follow best practices, the first step is to configure an IAM user group and user with the minimum necessary permissions to run this project. The IAM user group will have permissions to read and write to S3 buckets, interact with the EC2 instance, and access the RDS database. The following steps will be performed under the root user.

### Permissions needed for this project

- Read and write S3
- Permissions to EC2
- Permissions to RDS

### Create a user

1. Go to **IAM > Users > Create User**
2. Create a name for the user, e.g., `data-pipeline`
3. Attach this user to a group for best practices, as outlined by AWS, especially when replicating an instance where there are multiple users in a group
4. Create the user

### Create a user group

1. Go to **IAM > User groups > Create group**
2. Create a name for the group, e.g., `data-pipeline-group`
3. Add the user created to this group
4. Create the user group

### Create a policy

1. Go to **IAM > Policies > Create a policy**
2. Add the following permissions to the IAM policy:

    ```json
    {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "S3Access",
                "Effect": "Allow",
                "Action": [
                    "s3:GetObject",
                    "s3:PutObject",
                    "s3:ListBucket"
                ],
                "Resource": [
                    "arn:aws:s3:::your-bucket-name",
                    "arn:aws:s3:::your-bucket-name/*"
                ]
            },
            {
                "Sid": "RDSAccess",
                "Effect": "Allow",
                "Action": [
                    "rds:DescribeDBInstances",
                    "rds:DescribeDBClusters",
                    "rds:ModifyDBInstance",
                    "rds:ModifyDBCluster",
                    "rds-data:ExecuteStatement",
                    "rds-data:BatchExecuteStatement"
                ],
                "Resource": "arn:aws:rds:*:*:db:your-db-instance-identifier"
            }
        ]
    }
    ```

3. To ensure the IAM group only has access to the DAG and staging data S3 buckets and the RDS database, specify the `Resource` in the IAM JSON template. Since these resources haven’t been created yet, replace with `"Resource": "*"` temporarily. Once created, modify to point to the correct location.
4. Add a policy name, e.g., `data-pipeline-policy`.
5. Create the policy.

### Attach policy to group

1. Go to the **data-pipeline-group** created and select the **Permissions** tab
2. Click **Add permissions**
3. Search for `data-pipeline-policy`
4. Attach the policy

## Setting up S3 Bucket

Two S3 buckets will be created, one for staging data and one for DAGs. When following the steps, ensure you are in the correct region. For this project, `eu-west-2` will be used

1. Go to **S3 > Buckets > Create bucket > General purpose**
2. Add a bucket name, e.g., `data-pipeline-airflow-dags-2`
3. Create the bucket

Repeat the above steps to create a bucket for staging data with the name `data-pipeline-staging-data-2`.

## Setting up RDS

### Initial setup

The RDS will be used to store the final datasets

1. Go to **RDS > Create database**
2. Select the **PostgreSQL** database
3. Select the **Free tier** template
4. Add a database name, e.g., `data-pipeline-database`
5. Add a master username and password
6. Select `db.t3.micro` for the instance configuration
7. Select **General Purpose SSD (gp2)** for storage
8. Disable storage autoscaling
9. Select **Default VPC** for connectivity and **default** subnet group
10. Allow public access to the RDS
11. Under additional configuration, create an initial database name, e.g., `data_pipeline_db`
12. Create the database

### Setting up connectivity

1. Click on the database created
2. Click on the link under **Security > VPC security groups**
3. Go to **Inbound rules**
4. Click on **Edit inbound rules**
5. Add a rule:
    - Allow **Custom TCP**, port `8080`, `IPv4`
6. Save the rules

## Setting up EC2

### Initial setup

The EC2 instance will be used as a server for Airflow.

1. Go to **EC2 > Launch Instance**
2. Add a server name, e.g., `airflow-server`
3. Click on **Ubuntu** for the OS image
4. Leave the AMI as the free tier image
5. Select `t2.small` for the instance type to ensure enough memory to run Airflow
6. Create a new key-pair login, e.g., `data-pipeline-key-pair` for the RSA OpenSSH key pair
7. Under network settings, select the checkboxes:
    - Allow **HTTPS traffic** from the internet
    - Allow **HTTP traffic** from the internet
8. Launch the instance

### Setting up connectivity

1. Go to **Instances > Security**
2. Click on the security groups link
3. Click **Edit inbound rules**
4. Add a rule:
    - Allow **Custom TCP**, port `8080`, **My IP**
5. Save the rules

### Adding Elastic IP

An elastic IP stays static and will not change, which is useful.

1. On the EC2 dashboard, go to **Elastic IPs > Allocate Elastic IP address > Allocate**
2. Click on the airflow instance to associate it with the elastic IP

## Downloading AWS CLI

The AWS CLI will be used frequently when accessing AWS services via the command line. It can be downloaded by following the instructions on the AWS website or by running the following command:

```bash
curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
unzip awscliv2.zip
sudo ./aws/install

```

# Setting up Airflow

## Connecting to the EC2

1. Check the EC2 instance created and click connect

2. Using the downloaded key pair and the instructions given by AWS, connect to the EC2 instance. The SSH command will look similar to the one below:

    ```bash
    ssh -i "data-pipeline-key-pair.pem" ubuntu@{public_dns}
    ```
    After running the command using the key pair PEM file, the connection should be successful

## Downloading Airflow on the EC2

1. The first thing to do on the new EC2 instance is to run an update to install the latest packages:

    ```bash
    # Update the package list
    sudo apt-get update

    # Install Python, pip, and other necessary packages
    sudo apt-get install -y python3 python3-pip python3-dev libmysqlclient-dev build-essential
    ```

2. Next, create a virtual environment which will allow for the setup of the virtual environment:

    ```bash
    sudo apt install python3.12-venv
    python3 -m venv venv
    ```

3. Access the virtual Python environment with the following command:

    ```bash
    source venv/bin/activate
    ```

4. It's useful to upgrade `pip` in the virtual environment:

    ```bash
    pip install --upgrade pip setuptools wheel
    ```

5. Now, you can download Airflow:

    ```bash
    pip install apache-airflow
    ```

6. After downloading Airflow, you can run it as follows:

    - **Initialize the Database**:

        ```bash
        airflow db init
        ```

    - **Create an Airflow user**:

        ```bash
        airflow users create \
            --username admin \
            --password admin \
            --firstname First \
            --lastname Last \
            --role Admin \
            --email admin@example.com
        ```

    - **Start the Airflow webserver**:

        ```bash
        airflow webserver -p 8080 -D
        ```

    - **Start the Airflow scheduler**:

        ```bash
        airflow scheduler -D
        ```

7. To check if Airflow is running via the CLI, you can run the following command and look at the status:

    ```bash
    ps aux | grep airflow
    ```

8. Now that Airflow is up and running, the webserver UI can be accessed by going to the EC2 URL and opening it with the extension `:8080`, e.g., `http://your_ec2_public_ip:8080`. Another way to access the website is with the Elastic IP created, where the URL will be `http://your_elastic_ip:8080`

    ![main](image.png)

9. Once you have logged in with your credentials, you can access Airflow

10. To remove all the example DAGs that Airflow offers by default, go to the Airflow config file and set `load_examples = False`:

    ```bash
    nano ~/airflow/airflow.cfg
    ```

11. After setting this, source the command:

    ```bash
    source ~/airflow/airflow.cfg
    ```

12. After this, the Airflow services need to be restarted:

    ```bash
    # Restart the webserver
    pkill -f "airflow webserver"
    pkill -f "airflow scheduler"

    # Restart the webserver and scheduler
    airflow webserver -p 8080 -D
    airflow scheduler -D
    ```

13. Finally, to ensure Airflow starts up when the EC2 is turned on, run the following:

    ```bash
    # Create a systemd service file for the Airflow webserver
    sudo nano /etc/systemd/system/airflow-webserver.service
    ```

    Add this to the file:

    ```ini
    [Unit]
    Description=Airflow webserver daemon
    After=network.target

    [Service]
    User=ubuntu
    Group=ubuntu
    Environment="PATH=/home/ubuntu/venv/bin"
    ExecStart=/home/ubuntu/venv/bin/airflow webserver --pid /home/ubuntu/airflow/airflow-webserver.pid
    Restart=always
    RestartSec=5s

    [Install]
    WantedBy=multi-user.target
    ```

    Similarly:

    ```bash
    # Create a systemd service file for the Airflow scheduler
    sudo nano /etc/systemd/system/airflow-scheduler.service
    ```

    Add this to the file:

    ```ini
    [Unit]
    Description=Airflow scheduler daemon
    After=network.target

    [Service]
    User=ubuntu
    Group=ubuntu
    Environment="PATH=/home/ubuntu/venv/bin"
    ExecStart=/home/ubuntu/venv/bin/airflow scheduler --pid /home/ubuntu/airflow/airflow-scheduler.pid
    Restart=always
    RestartSec=5s

    [Install]
    WantedBy=multi-user.target
    ```

14. When booting up, run:

    ```bash
    # Enable services
    sudo systemctl enable airflow-webserver
    sudo systemctl enable airflow-scheduler

    # Start services
    sudo systemctl start airflow-webserver
    sudo systemctl start airflow-scheduler
    ```

15. Verify the services are running:

    ```bash
    sudo systemctl status airflow-webserver
    sudo systemctl status airflow-scheduler
    ```

16. To ensure this variable is available when trying to run DAGs with a config or at a different timestamp, ensure `show_trigger_form_if_no_params = False` is set to `True` in the airflow.cfg file

## Downloading AWS CLI on EC2

1. Run the commands found in the `Downloading AWS CLI` section.

## Set up IAM Credentials on EC2

1. Now the AWS credentials need to be set up to allow the EC2 instance to interact with the other AWS services like the S3 bucket and RDS

2. Go to **IAM > Users**

3. Create an access key and save the access key details

4. In the bashrc file of the EC2 instance, export the access key variables and source them:

    ```bash
    export AWS_ACCESS_KEY_ID='your_access_key'
    export AWS_SECRET_ACCESS_KEY='your_secret_key'
    ```

5. Once downloaded, configure AWS with your access key and secret key on the EC2 by typing:

    ```bash
    aws configure
    ```

6. After, ensure the following Airflow AWS package is installed to interact with AWS services:

    ```bash
    pip install apache-airflow-providers-amazon
    ```

# Setting up CI/CD

## GitHub Actions Pipeline

1. The GitHub actions pipeline needs to:

    - Push new DAG and requirements changes to the DAGs S3 bucket
    - Connect to the EC2 instance
    - Sync S3 bucket changes to the EC2 Airflow

2. Set up:

    - Go to the GitHub repo with your DAGs and other files and click on the settings button
    - Navigate to **Secrets and variables > Actions > New repository secret**

3. Add the following variables in secrets to ensure the pipeline can run the AWS services as intended:

    - `AWS_ACCESS_KEY_ID`: Your AWS access key ID
    - `AWS_SECRET_ACCESS_KEY`: Your AWS secret access key
    - `AWS_REGION`: The AWS region where your S3 bucket is located
    - `S3_BUCKET`: The name of your S3 bucket
    - `EC2_INSTANCE`: The public DNS or IP address of your EC2 instance (better yet, use the Elastic IP address so it does not change. If set up, it will do it automatically)
    - `SSH_PRIVATE_KEY`: Your EC2 instance's SSH private key
    - `EC2_USER`: The username for SSH access to your EC2 instance

4. After adding the keys, create a GitHub Actions workflow:

    - Create a new workflow file in the repository at `.github/workflows/deploy-dags.yml`

5. In this example, the pipeline created can be found under `.github/workflows/deploy-dags.yml`. As soon as the YAML file is written and the code is pushed, it will be initiated

6. At this point in the project, if code is pushed to the repo, the changes will be reflected on Airflow, where the DAGs will update on the local Airflow server and the packages will also be updated if there is a change. The infrastructure has been done and what is left is the setup of the DAGs

## Variables on Airflow

1. The following variables need to be added to Airflow via the UI under the variables tab to ensure the codebase and pipeline work as intended:

    - S3 staging bucket path so that the variable can be used in the Python code when trying to ingest and retrieve from the bucket
    - RDS credentials so that the staging data can be loaded into the database using `boto3`
