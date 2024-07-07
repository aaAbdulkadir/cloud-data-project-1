# Pipeline setup

**Note**: Everything is being done with the free tier of AWS.


## Setting up IAM

The first thing to do is to set up an IAM user group and user so that you can enable the bare minimum permisssions needed to get this project running, following best practices. The user group that will be created is for a developer that is able to read and write to the s3 buckets and RDS database. The following series of steps will be done in the root user.


**Permissions needed for this project**:

- Read write s3

- Permissions to EC2

- Permissions to RDS


**Create a user**:

- IAM > Users > Create User 

-  Create a name for the user, in this case the name will be  `data-pipeline`

- We will attach this user to a group for best practices, as outlined by AWS, or replicating an instance where there are mulitple users in a group

- Create user


**Create a user group**:

- IAM > User groups > Create group

- Create a name for the group, in this case the name will be `data-pipeline-group`

- Add the user created to this group 

- Create user group

**Create a policy**:

- Create a policy which has the necessary permissions needed

- IAM > Policies > Create a policy

- Add the following permissions to the IAM policy:
```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "S3ReadWriteAccess",
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:ListBucket",
                "s3:PutObject",
                "s3:DeleteObject"
            ],
            "Resource": [
                "arn:aws:s3:::your-bucket-name",
                "arn:aws:s3:::your-bucket-name/*"
            ]
        },
        {
            "Sid": "RDSReadWriteAccess",
            "Effect": "Allow",
            "Action": [
                "rds:ModifyDBInstance",
                 **DOUBLE CHECK THIS RDS STUFF**
            ],
            "Resource": [
                "arn:aws:rds:<region>:<account-id>:db:<db-instance-name>"
            ]
        },
        { ** DONT NEED THIS **
            "Sid": "EC2Access",
            "Effect": "Allow",
            "Action": [
                "ec2:StartInstances",
                "ec2:StopInstances",
                "ec2:RebootInstances",
                "ec2:TerminateInstances"
            ],
            "Resource": [
                "arn:aws:ec2:<region>:<account-id>:instance/<instance-id>"
            ]
        }
    ]
}

```

- We want the IAM group to only have access to the dag and staging data s3 buckets, the RDS database and the EC2 instance where Airflow will be provisioned, nothing else. to ensure that, the resource has to be stated in the IAM json template. As for now, those have not been created so it can be replaced with `"Resource": "*"` and once created, this can be modified.


- Add policty name, in this case the name will be `data-pipeline-policy`

- Create policy


**Attach policy to group**:

- Go to the `data-pipeline-group` created and select the permissions tab

- Add permissions

- Search for `data-pipeline-policy`

- Attach policies
