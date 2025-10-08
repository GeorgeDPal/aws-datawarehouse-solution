# Lambda 3 – Provision Redshift Serverless, create tables, and load curated parquet data
import boto3
import logging
import os
import time
import traceback

# ---------------- Logging Setup ----------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# ---------------- AWS Clients ----------------
redshift = boto3.client("redshift-serverless")
redshift_data = boto3.client("redshift-data")
glue = boto3.client("glue")
sts = boto3.client("sts")

def wait_for_workgroup_available(workgroup, max_seconds=900, poll_interval=15):
    waited = 0
    while True:
        wg = redshift.get_workgroup(workgroupName=workgroup)["workgroup"]
        status = wg.get("status")
        logger.info(f"Workgroup status = {status}")
        if status == "AVAILABLE":
            return True
        if waited >= max_seconds:
            raise TimeoutError(f"Workgroup {workgroup} not AVAILABLE after {max_seconds}s")
        time.sleep(poll_interval)
        waited += poll_interval

def wait_for_crawler_ready(crawler_name, max_seconds=600, poll_interval=15):
    waited = 0
    while True:
        crawler = glue.get_crawler(Name=crawler_name)["Crawler"]
        state = crawler.get("State")
        logger.info(f"Crawler '{crawler_name}' state = {state}")
        # Glue states: RUNNING, READY, STOPPING, etc. We want READY (idle)
        if state == "READY":
            return True
        if waited >= max_seconds:
            raise TimeoutError(f"Crawler {crawler_name} not READY after {max_seconds}s")
        time.sleep(poll_interval)
        waited += poll_interval

def execute_sql(workgroup, database, sql):
    resp = redshift_data.execute_statement(WorkgroupName=workgroup, Database=database, Sql=sql)
    stmt_id = resp.get("Id")
    logger.info(f"Submitted SQL. statement_id={stmt_id}")
    return stmt_id

def lambda_handler(event, context):
    try:
        account_id = sts.get_caller_identity()["Account"]

        # -----------------------
        # Environment variables
        # -----------------------
        bucket = os.environ["BUCKET_NAME"]
        crawler = os.environ["CRAWLER_NAME"]
        db_name = os.environ["DATABASE_NAME"]        # Glue Catalog db
        role_name = os.environ["GLUE_CRAWLER_ROLE"]  # IAM role name only (e.g. glue-crawler-role)
        namespace = os.environ["REDSHIFT_NAMESPACE"]
        workgroup = os.environ["REDSHIFT_WORKGROUP"]
        db = os.environ["REDSHIFT_DATABASE"]
        admin_user = os.environ["REDSHIFT_USER"]
        admin_pass = os.environ["REDSHIFT_PASSWORD"]

        role_arn = f"arn:aws:iam::{account_id}:role/{role_name}"
        copy_role = f"arn:aws:iam::{account_id}:role/redshift-copy-role"

        logger.info("🚀 Lambda 3 triggered - starting Redshift + Glue setup.")

        # -----------------------
        # Ensure Redshift Namespace
        # -----------------------
        try:
            redshift.get_namespace(namespaceName=namespace)
            logger.info(f"ℹ️ Namespace {namespace} already exists")
        except redshift.exceptions.ResourceNotFoundException:
            logger.info(f"Namespace {namespace} not found. Creating...")
            redshift.create_namespace(
                namespaceName=namespace,
                adminUsername=admin_user,
                adminUserPassword=admin_pass,
                iamRoles=[copy_role]
            )
            logger.info(f"✅ Created Redshift namespace: {namespace}")

        # -----------------------
        # Ensure Redshift Workgroup
        # -----------------------
        try:
            redshift.get_workgroup(workgroupName=workgroup)
            logger.info(f"ℹ️ Workgroup {workgroup} already exists")
        except redshift.exceptions.ResourceNotFoundException:
            logger.info(f"Workgroup {workgroup} not found. Creating...")
            redshift.create_workgroup(
                workgroupName=workgroup,
                namespaceName=namespace,
                baseCapacity=8
            )
            logger.info(f"✅ Created Redshift workgroup: {workgroup}")

        # Wait until workgroup is AVAILABLE (bounded wait)
        logger.info("⏳ Waiting for workgroup to become AVAILABLE...")
        wait_for_workgroup_available(workgroup, max_seconds=900, poll_interval=15)
        logger.info("✅ Redshift workgroup AVAILABLE")

        # -----------------------
        # Ensure Glue Database exists
        # -----------------------
        try:
            glue.get_database(Name=db_name)
            logger.info(f"ℹ️ Glue database '{db_name}' already exists")
        except glue.exceptions.EntityNotFoundException:
            logger.info(f"Glue database '{db_name}' not found. Creating...")
            glue.create_database(DatabaseInput={"Name": db_name})
            logger.info(f"✅ Created Glue database: {db_name}")

        # -----------------------
        # Ensure Glue Crawler
        # -----------------------
        try:
            glue.get_crawler(Name=crawler)
            logger.info(f"ℹ️ Glue crawler '{crawler}' already exists")
        except glue.exceptions.EntityNotFoundException:
            logger.info(f"Glue crawler '{crawler}' not found. Creating...")
            try:
                glue.create_crawler(
                    Name=crawler,
                    Role=role_arn,
                    DatabaseName=db_name,
                    Targets={"S3Targets": [{"Path": f"s3://{bucket}/curated/"}]},
                    TablePrefix="curated_"
                )
                logger.info(f"✅ Created crawler '{crawler}' successfully")
            except Exception as create_err:
                logger.error(f"❌ Failed to create crawler '{crawler}': {create_err}")
                raise

        # Start crawler and wait for it to finish
        try:
            glue.start_crawler(Name=crawler)
            logger.info(f"🚀 Started crawler '{crawler}'")
        except Exception as start_err:
            logger.error(f"❌ Failed to start crawler '{crawler}': {start_err}")
            raise

        logger.info("⏳ Waiting for crawler to finish (state=READY)...")
        wait_for_crawler_ready(crawler, max_seconds=600, poll_interval=15)
        logger.info("✅ Crawler finished and metadata refreshed")

        # -----------------------
        # Create Redshift Tables (idempotent)
        # -----------------------
        ddl_statements = [
            """
            CREATE TABLE IF NOT EXISTS dim_product (
                product_name VARCHAR(255),
                category VARCHAR(255),
                PRIMARY KEY(product_name)
            )
            DISTSTYLE ALL;
            """,
            """
            CREATE TABLE IF NOT EXISTS dim_date (
                crawl_year INT,
                crawl_month INT,
                PRIMARY KEY(crawl_year, crawl_month)
            )
            DISTSTYLE ALL;
            """,
            """
            CREATE TABLE IF NOT EXISTS fact_sales (
                product_name VARCHAR(255),
                current_discounted_price DOUBLE PRECISION,
                listed_price DOUBLE PRECISION,
                rating DOUBLE PRECISION,
                number_of_reviews DOUBLE PRECISION,
                discount_amount DOUBLE PRECISION,
                discount_flag INT,
                rating_bucket VARCHAR(50),
                crawl_year INT,
                crawl_month INT,
                FOREIGN KEY (product_name) REFERENCES dim_product(product_name),
                FOREIGN KEY (crawl_year, crawl_month) REFERENCES dim_date(crawl_year, crawl_month)
            )
            DISTSTYLE KEY
            DISTKEY(product_name)
            SORTKEY(crawl_year, crawl_month);
            """
        ]

        for ddl in ddl_statements:
            try:
                stmt_id = execute_sql(workgroup, db, ddl)
                logger.info(f"DDL executed (stmt_id={stmt_id})")
            except Exception as ddl_err:
                logger.error(f"❌ Error executing DDL: {ddl_err}")
                logger.error(traceback.format_exc())
                raise

        logger.info("✅ Redshift tables ensured")

        # -----------------------
        # TRUNCATE + COPY Load
        # -----------------------
        tables = ["dim_product", "dim_date", "fact_sales"]
        for t in tables:
            try:
                stmt_id = execute_sql(workgroup, db, f"TRUNCATE TABLE {t};")
                logger.info(f"Truncate submitted for {t} (stmt_id={stmt_id})")
            except Exception as trunc_err:
                logger.error(f"❌ Failed to truncate {t}: {trunc_err}")
                logger.error(traceback.format_exc())
                raise

        copy_statements = [
            f"""
            COPY dim_product 
            FROM 's3://{bucket}/curated/dim_product/' 
            IAM_ROLE '{copy_role}' 
            FORMAT AS PARQUET;
            """,
            f"""
            COPY dim_date 
            FROM 's3://{bucket}/curated/dim_date/' 
            IAM_ROLE '{copy_role}' 
            FORMAT AS PARQUET;
            """,
            f"""
            COPY fact_sales  
            FROM 's3://{bucket}/curated/fact_sales/'  
            IAM_ROLE '{copy_role}' 
            FORMAT AS PARQUET;
            """
        ]

        for sql in copy_statements:
            try:
                stmt_id = execute_sql(workgroup, db, sql)
                logger.info(f"COPY submitted (stmt_id={stmt_id})")
            except Exception as copy_err:
                logger.error(f"❌ COPY failed: {copy_err}")
                logger.error(traceback.format_exc())
                raise

        logger.info("🎯 Data successfully loaded into Redshift.")
        return {"status": "success"}

    except Exception as e:
        logger.error(f"💥 Lambda 3 failed: {e}")
        logger.error(traceback.format_exc())
        return {"status": "error", "message": str(e)}
