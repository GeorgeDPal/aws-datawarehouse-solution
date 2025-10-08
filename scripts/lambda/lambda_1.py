import boto3
import logging
import os
import traceback

# Logging setup
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger()
glue = boto3.client("glue")

def lambda_handler(event, context):
    logger.info("🚀 Lambda triggered via EventBridge.")
    logger.info(f"Incoming event: {event}")

    try:
        job_name = os.environ.get("GLUE_JOB_NAME")
        if not job_name:
            raise ValueError("❌ Missing environment variable: GLUE_JOB_NAME")

        logger.info(f"🧩 Attempting to start Glue job: {job_name}")
        response = glue.start_job_run(JobName=job_name)
        job_run_id = response.get("JobRunId")

        logger.info(f"✅ Glue job '{job_name}' started successfully! Run ID: {job_run_id}")
        return {"status": "success", "JobRunId": job_run_id}

    except glue.exceptions.EntityNotFoundException:
        logger.error(f"❌ Glue job '{job_name}' not found in region {glue.meta.region_name}.")
    except glue.exceptions.AccessDeniedException:
        logger.error(f"🚫 Access denied: Lambda’s role lacks Glue permissions.")
    except Exception as e:
        logger.error(f"💥 Unexpected error: {e}")
        logger.error(traceback.format_exc())

    return {"status": "error", "message": "Failed to start Glue job"}
