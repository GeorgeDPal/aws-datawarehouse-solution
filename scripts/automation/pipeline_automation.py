import boto3
import logging
import sys
from botocore.exceptions import ClientError

# -----------------------
# Logging setup
# -----------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

# -----------------------
# AWS clients
# -----------------------
s3 = boto3.client("s3")
lambda_client = boto3.client("lambda")
sts = boto3.client("sts")

# -----------------------
# Configuration
# -----------------------
ACCOUNT_ID = sts.get_caller_identity()["Account"]
REGION = boto3.session.Session().region_name or "us-east-1"
BUCKET_NAME = "dp-datawarehouse-solution-1"

LAMBDA_2_NAME = "lambda-split-fact-dim"
LAMBDA_3_NAME = "lambda-load-redshift"

# -----------------------
# 1️⃣ Grant permission for S3 → Lambda 2
# -----------------------
def add_s3_permission_to_lambda():
    """Allow S3 bucket to invoke lambda-split-fact-dim"""
    try:
        lambda_client.add_permission(
            FunctionName=LAMBDA_2_NAME,
            StatementId="s3invoke-transformed",
            Action="lambda:InvokeFunction",
            Principal="s3.amazonaws.com",
            SourceArn=f"arn:aws:s3:::{BUCKET_NAME}",
            SourceAccount=ACCOUNT_ID
        )
        logger.info("✅ Added S3 invoke permission for lambda-split-fact-dim")
    except ClientError as e:
        if "ResourceConflictException" in str(e):
            logger.info("ℹ️ S3 invoke permission already exists for lambda-split-fact-dim")
        else:
            logger.error(f"❌ Failed to add S3 invoke permission: {e}")
            raise

# -----------------------
# 2️⃣ Add S3 event notification (transformed/ → Lambda 2)
# -----------------------
def add_s3_event_notification():
    """Configure S3 event to trigger Lambda 2 on transformed/ prefix"""
    try:
        existing_config = s3.get_bucket_notification_configuration(Bucket=BUCKET_NAME)
    except ClientError as e:
        logger.error("❌ Failed to fetch S3 bucket notification config: %s", e)
        sys.exit(1)

    # 🧹 Remove invalid metadata
    existing_config.pop("ResponseMetadata", None)

    lambda_arn = f"arn:aws:lambda:{REGION}:{ACCOUNT_ID}:function:{LAMBDA_2_NAME}"

    new_notification = {
        "Id": "trigger-lambda2-on-transform",
        "LambdaFunctionArn": lambda_arn,
        "Events": ["s3:ObjectCreated:*"],
        "Filter": {
            "Key": {"FilterRules": [{"Name": "prefix", "Value": "transformed/"}]}
        }
    }

    # Check if already exists
    existing_lambdas = existing_config.get("LambdaFunctionConfigurations", [])
    already_exists = any(cfg.get("LambdaFunctionArn") == lambda_arn for cfg in existing_lambdas)

    if already_exists:
        logger.info("ℹ️ S3 → Lambda 2 notification already configured.")
        return

    # Append and update
    updated_config = existing_config
    updated_config.setdefault("LambdaFunctionConfigurations", []).append(new_notification)

    try:
        s3.put_bucket_notification_configuration(
            Bucket=BUCKET_NAME,
            NotificationConfiguration=updated_config
        )
        logger.info("✅ Added S3 event trigger: transformed/ → lambda-split-fact-dim")
    except ClientError as e:
        logger.error("❌ Failed to add S3 event trigger: %s", e)
        sys.exit(1)

# -----------------------
# 3️⃣ Ensure Lambda 2 triggers Lambda 3
# -----------------------
def ensure_lambda2_invokes_lambda3():
    """Verify lambda_2.py triggers lambda-load-redshift"""
    logger.info("🔍 Verifying lambda_2.py includes Lambda 3 trigger code...")
    path = r"D:\datawarehouse-solution\scripts\lambda\lambda_2.py"

    try:
        # ✅ open safely with utf-8 encoding to avoid UnicodeDecodeError
        with open(path, "r", encoding="utf-8") as f:
            code = f.read()
    except FileNotFoundError:
        logger.error(f"❌ Could not find lambda_2.py at {path}")
        return
    except UnicodeDecodeError:
        logger.warning("⚠️ lambda_2.py has non-UTF8 characters; retrying with 'latin-1' fallback...")
        with open(path, "r", encoding="latin-1") as f:
            code = f.read()

    if "lambda_client.invoke(" in code and "lambda-load-redshift" in code:
        logger.info("✅ lambda_2.py already triggers lambda-load-redshift")
    else:
        logger.warning("⚠️ lambda_2.py does NOT currently invoke lambda-load-redshift.")
        logger.info("➡️ Add this snippet inside lambda_handler():\n")
        print(
            "    try:\n"
            "        lambda_client = boto3.client('lambda')\n"
            "        lambda_client.invoke(\n"
            "            FunctionName='lambda-load-redshift',\n"
            "            InvocationType='Event'\n"
            "        )\n"
            "        logger.info('🚀 Invoked lambda-load-redshift to load curated data into Redshift')\n"
            "    except Exception as e:\n"
            "        logger.error(f'❌ Failed to trigger lambda-load-redshift: {e}')"
        )

# -----------------------
# Main
# -----------------------
def main():
    logger.info("🔧 Connecting pipeline events (S3 → Lambda 2 → Lambda 3)...")
    add_s3_permission_to_lambda()
    add_s3_event_notification()
    ensure_lambda2_invokes_lambda3()
    logger.info("✅ Pipeline event wiring complete!")

if __name__ == "__main__":
    main()
