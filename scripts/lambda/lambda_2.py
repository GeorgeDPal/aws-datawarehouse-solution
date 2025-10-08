import boto3
import logging
import os
import traceback

# ---------------- Logging Setup ----------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# ---------------- AWS Clients ----------------
glue = boto3.client("glue")

def lambda_handler(event, context):
    logger.info("🚀 Lambda 2 triggered")
    logger.info(f"📨 Incoming event: {event}")

    try:
        # Get bucket from environment
        bucket = os.environ.get("BUCKET_NAME")
        if not bucket:
            raise EnvironmentError("❌ Missing required environment variable: BUCKET_NAME")

        for record in event.get("Records", []):
            key = record["s3"]["object"]["key"]

            # Only process transformed parquet files
            if not key.startswith("transformed/") or not key.endswith(".parquet"):
                logger.info(f"⏩ Skipping non-transformed file: {key}")
                continue

            logger.info(f"🎯 Starting Glue job 'glue-split-fact-dim' for s3://{bucket}/{key}")

            # Fire-and-forget → just start Glue job 2
            response = glue.start_job_run(
                JobName="glue-split-fact-dim",
                Arguments={
                    "--BUCKET_NAME": bucket,
                    "--INPUT_KEY": "transformed/"
                }
            )
            job_run_id = response['JobRunId']
            logger.info(f"✅ Glue job started. JobRunId: {job_run_id}")

        logger.info("🎯 Lambda 2 finished (Glue job running in background).")
        return {"status": "success", "message": "Glue job glue-split-fact-dim started."}

    except Exception as e:
        logger.error(f"💥 Lambda 2 failed: {e}")
        logger.error(traceback.format_exc())
        return {"status": "error", "message": str(e)}
