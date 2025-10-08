import boto3
import logging
import time

# -----------------------
# Logging setup
# -----------------------
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# -----------------------
# AWS clients
# -----------------------
redshift = boto3.client("redshift-serverless")
redshift_data = boto3.client("redshift-data")
sts = boto3.client("sts")
account_id = sts.get_caller_identity()["Account"]

# -----------------------
# Config
# -----------------------
NAMESPACE_NAME = "dw-namespace"
WORKGROUP_NAME = "dw-workgroup"
DB_NAME = "dev"

# Admin user (created with namespace)
ADMIN_USER = "awsuser"
ADMIN_PASSWORD = "Password123!"   # ⚠️ use Secrets Manager in production

# BI user for Power BI
BI_USER = "bi_user"
BI_PASSWORD = "BIpassword123!"    # ⚠️ store securely, not hardcoded

IAM_ROLE = f"arn:aws:iam::{account_id}:role/redshift-copy-role"
LAMBDA_ROLE = f"arn:aws:iam::{account_id}:role/lambda-etl-role"

# -----------------------
# Create namespace
# -----------------------
def create_namespace():
    try:
        redshift.get_namespace(namespaceName=NAMESPACE_NAME)
        logger.info(f"ℹ️ Namespace {NAMESPACE_NAME} already exists")
    except redshift.exceptions.ResourceNotFoundException:
        try:
            redshift.create_namespace(
                namespaceName=NAMESPACE_NAME,
                adminUsername=ADMIN_USER,
                adminUserPassword=ADMIN_PASSWORD,
                iamRoles=[IAM_ROLE]
            )
            logger.info(f"✅ Created namespace {NAMESPACE_NAME}")
        except Exception as e:
            logger.exception("Failed to create namespace: %s", e)
            raise
# -----------------------
# Create workgroup
# -----------------------
def create_workgroup():
    try:
        redshift.get_workgroup(workgroupName=WORKGROUP_NAME)
        logger.info(f"ℹ️ Workgroup {WORKGROUP_NAME} already exists")
    except redshift.exceptions.ResourceNotFoundException:
        try:
            redshift.create_workgroup(
                workgroupName=WORKGROUP_NAME,
                namespaceName=NAMESPACE_NAME,
                baseCapacity=8  # 8 RPU = minimum (~$0.24/hr)
            )
            logger.info(f"✅ Created workgroup {WORKGROUP_NAME}")
        except Exception as e:
            logger.exception("Failed to create workgroup: %s", e)
            raise

# -----------------------
# Wait until available
# -----------------------
def wait_for_workgroup(timeout_minutes=20):
    logger.info("Waiting for workgroup %s to become AVAILABLE (timeout %s minutes)...", WORKGROUP_NAME, timeout_minutes)
    start = time.time()
    timeout = timeout_minutes * 60
    while True:
        wg = redshift.get_workgroup(workgroupName=WORKGROUP_NAME)["workgroup"]
        status = wg.get("status")
        logger.debug("Workgroup status: %s", status)
        if status in ("AVAILABLE", "ACTIVE"):
            logger.info("Workgroup is AVAILABLE/ACTIVE")
            return
        if time.time() - start > timeout:
            raise TimeoutError(f"Timed out waiting for workgroup {WORKGROUP_NAME} (last status: {status})")
        time.sleep(15)

# -----------------------
# Create BI user
# -----------------------
def create_bi_user():
    logger.info("👤 Creating BI user for analytics...")
    sql_statements = [
        f"CREATE USER IF NOT EXISTS {BI_USER} PASSWORD '{BI_PASSWORD}';",
        f"GRANT USAGE ON SCHEMA public TO {BI_USER};",
        f"GRANT SELECT ON ALL TABLES IN SCHEMA public TO {BI_USER};",
        f"ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO {BI_USER};"
    ]

    for sql in sql_statements:
        redshift_data.execute_statement(
            WorkgroupName=WORKGROUP_NAME,
            Database=DB_NAME,
            Sql=sql
        )
    logger.info(f"✅ BI user {BI_USER} created and granted SELECT privileges")

# -----------------------
# Main
# -----------------------
def main():
    create_namespace()
    create_workgroup()
    wait_for_workgroup()
    create_bi_user()

if __name__ == "__main__":
    main()
