import boto3
import json
import logging

# ----------------------------
# Logging setup
# ----------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

# ----------------------------
# AWS clients
# ----------------------------
iam = boto3.client("iam")
sts = boto3.client("sts")
account_id = sts.get_caller_identity()["Account"]

# ----------------------------
# Helper function
# ----------------------------
def create_role_if_not_exists(role_name, assume_policy, description):
    """
    Creates IAM role if not exists. Returns role ARN.
    """
    try:
        iam.get_role(RoleName=role_name)
        logger.info(f"ℹ️ Role {role_name} already exists")
        return f"arn:aws:iam::{account_id}:role/{role_name}"
    except iam.exceptions.NoSuchEntityException:
        role = iam.create_role(
            RoleName=role_name,
            AssumeRolePolicyDocument=json.dumps(assume_policy),
            Description=description
        )
        logger.info(f"✅ Created role {role_name}")
        return role["Role"]["Arn"]


def attach_policies(role_name, policy_arns):
    """
    Attaches managed policies to an IAM role.
    """
    for policy_arn in policy_arns:
        iam.attach_role_policy(RoleName=role_name, PolicyArn=policy_arn)
        logger.info(f"🔗 Attached {policy_arn.split('/')[-1]} to {role_name}")


# ----------------------------
# 1️⃣ Glue ETL Role
# ----------------------------
glue_assume_policy = {
    "Version": "2012-10-17",
    "Statement": [
        {"Effect": "Allow", "Principal": {"Service": "glue.amazonaws.com"}, "Action": "sts:AssumeRole"}
    ]
}

glue_role_arn = create_role_if_not_exists(
    "glue-etl-role",
    glue_assume_policy,
    "Role for Glue ETL"
)

attach_policies("glue-etl-role", [
    "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole",
    "arn:aws:iam::aws:policy/AmazonS3FullAccess",
    "arn:aws:iam::aws:policy/CloudWatchLogsFullAccess"
])

# ----------------------------
# 2️⃣ Lambda ETL Role
# ----------------------------
lambda_assume_policy = {
    "Version": "2012-10-17",
    "Statement": [
        {"Effect": "Allow", "Principal": {"Service": "lambda.amazonaws.com"}, "Action": "sts:AssumeRole"}
    ]
}

lambda_role_arn = create_role_if_not_exists(
    "lambda-etl-role",
    lambda_assume_policy,
    "Role for all Lambda functions in ETL pipeline"
)

attach_policies("lambda-etl-role", [
    "arn:aws:iam::aws:policy/AmazonS3FullAccess",
    "arn:aws:iam::aws:policy/AWSGlueConsoleFullAccess",
    "arn:aws:iam::aws:policy/AmazonRedshiftFullAccess",
    "arn:aws:iam::aws:policy/CloudWatchLogsFullAccess"
])

# ➕ Add inline policy to allow Lambda-to-Lambda invocation
lambda_invoke_policy = {
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": ["lambda:InvokeFunction"],
            "Resource": "*"
        }
    ]
}
iam.put_role_policy(
    RoleName="lambda-etl-role",
    PolicyName="AllowLambdaInvoke",
    PolicyDocument=json.dumps(lambda_invoke_policy)
)
logger.info("🔗 Inline policy added to allow Lambda → Lambda invocation")


# ----------------------------
# 3️⃣ Redshift COPY Role
# ----------------------------
redshift_assume_policy = {
    "Version": "2012-10-17",
    "Statement": [
        {"Effect": "Allow", "Principal": {"Service": "redshift.amazonaws.com"}, "Action": "sts:AssumeRole"}
    ]
}

redshift_role_arn = create_role_if_not_exists(
    "redshift-copy-role",
    redshift_assume_policy,
    "Role for Redshift COPY command from S3"
)

attach_policies("redshift-copy-role", [
    "arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
])


# ----------------------------
# 4️⃣ Glue Crawler Role
# ----------------------------
crawler_assume_policy = {
    "Version": "2012-10-17",
    "Statement": [
        {"Effect": "Allow", "Principal": {"Service": "glue.amazonaws.com"}, "Action": "sts:AssumeRole"}
    ]
}

crawler_role_arn = create_role_if_not_exists(
    "glue-crawler-role",
    crawler_assume_policy,
    "Role for Glue Crawler"
)

attach_policies("glue-crawler-role", [
    "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole",
    "arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
])


# ----------------------------
# ✅ Final Summary
# ----------------------------
logger.info("\n🎯 All IAM roles created and updated successfully:")
logger.info(f"🔹 Glue ETL Role ARN:       {glue_role_arn}")
logger.info(f"🔹 Lambda ETL Role ARN:     {lambda_role_arn}")
logger.info(f"🔹 Redshift COPY Role ARN:  {redshift_role_arn}")
logger.info(f"🔹 Glue Crawler Role ARN:   {crawler_role_arn}")
