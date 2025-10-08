import boto3
import zipfile
import os
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

lambda_client = boto3.client("lambda")
iam = boto3.client("iam")

ROLE_NAME = "lambda-etl-role"

# ---------------------------------
# Utility: zip a lambda file
# ---------------------------------
def zip_lambda(source_file, zip_file):
    with zipfile.ZipFile(zip_file, "w") as z:
        z.write(source_file, os.path.basename(source_file))
    return zip_file

# ---------------------------------
# Deploy or update a lambda
# ---------------------------------
def create_or_update_lambda(function_name, handler, zip_file, timeout=300, env_vars=None):
    role_arn = iam.get_role(RoleName=ROLE_NAME)["Role"]["Arn"]

    with open(zip_file, "rb") as f:
        code_bytes = f.read()

    try:
        # Try to create
        response = lambda_client.create_function(
            FunctionName=function_name,
            Runtime="python3.11",
            Role=role_arn,
            Handler=handler,
            Code={"ZipFile": code_bytes},
            Timeout=timeout,
            Environment={"Variables": env_vars or {}}
        )
        logger.info(f"✅ Created Lambda: {function_name}")
    except lambda_client.exceptions.ResourceConflictException:
        # If exists, update code + config
        lambda_client.update_function_code(
            FunctionName=function_name, ZipFile=code_bytes
        )
        lambda_client.update_function_configuration(
            FunctionName=function_name,
            Role=role_arn,
            Handler=handler,
            Timeout=timeout,
            Environment={"Variables": env_vars or {}}
        )
        logger.info(f"ℹ️ Updated Lambda: {function_name}")

# ---------------------------------
# Main deploy
# ---------------------------------
if __name__ == "__main__":
    # Lambda 1
    zip1 = zip_lambda("lambda_1.py", "lambda_1.zip")
    create_or_update_lambda(
        "lambda-trigger-glue",
        "lambda_1.lambda_handler",
        zip1,
        timeout=300,
        env_vars={"GLUE_JOB_NAME": "glue-clean-transform"}
    )

    # Lambda 2 (no pandas!)
    zip2 = zip_lambda("lambda_2.py", "lambda_2.zip")
    create_or_update_lambda(
        "lambda-split-fact-dim",
        "lambda_2.lambda_handler",
        zip2,
        timeout=300,
        env_vars={"BUCKET_NAME": "dp-datawarehouse-solution-1"}
    )

    # Lambda 3 (needs long timeout)
    zip3 = zip_lambda("lambda_3.py", "lambda_3.zip")
    create_or_update_lambda(
        "lambda-load-redshift",
        "lambda_3.lambda_handler",
        zip3,
        timeout=900,  # ⬅️ Increased to 15 minutes
        # deploy_lambdas.py  (Lambda 3 envs)
    env_vars={
        "BUCKET_NAME": "dp-datawarehouse-solution-1",
        "CRAWLER_NAME": "product-data-crawler",
        "DATABASE_NAME": "product_db",             # <- rename from GLUE_DATABASE
        "GLUE_CRAWLER_ROLE": "glue-crawler-role",  # <- add this
        "REDSHIFT_NAMESPACE": "dw-namespace",      # see item (2)
        "REDSHIFT_WORKGROUP": "dw-workgroup",      # see item (2)
        "REDSHIFT_DATABASE": "dev",                # <- rename from REDSHIFT_DB
        "REDSHIFT_USER": "awsuser",
        "REDSHIFT_PASSWORD": "Password123!"        # (use Secrets Manager later)
    }

    )
    logger.info("🚀 All Lambdas deployed/updated.")