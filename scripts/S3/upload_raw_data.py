import os
import boto3
import botocore.exceptions
import kagglehub as kh
import logging
import shutil

# -----------------------
# Logging setup
# -----------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

# -----------------------
# Configuration
# -----------------------
BUCKET_NAME = "dp-datawarehouse-solution-1"
S3_KEY = "raw/amazon_products_sales_data_uncleaned.csv"
LOCAL_DATASET_DIR = r"D:\datawarehouse-solution\dataset"
LOCAL_FILE_NAME = "amazon_products_sales_data_uncleaned.csv"

# Detect AWS region automatically (fallback to us-east-1)
session = boto3.session.Session()
region = session.region_name or "us-east-1"
s3 = session.client("s3", region_name=region)

# -----------------------
# Functions
# -----------------------

def download_from_kaggle():
    """Download dataset from Kaggle and return the local path of the uncleaned CSV."""
    logger.info("⬇️ Downloading dataset from Kaggle...")
    path = kh.dataset_download("ikramshah512/amazon-products-sales-dataset-42k-items-2025")
    logger.info(f"📂 Dataset cached at: {path}")

    files = os.listdir(path)
    logger.info("📄 Files in dataset:")
    for f in files:
        logger.info(f" - {f}")

    for f in files:
        if "uncleaned" in f.lower() and f.endswith(".csv"):
            file_path = os.path.join(path, f)
            size = os.path.getsize(file_path)
            logger.info(f"✅ Selected uncleaned CSV: {file_path} ({size:,} bytes)")
            return file_path

    raise FileNotFoundError(f"❌ Could not find uncleaned CSV in {path}")


def upload_to_s3(file_path):
    """Upload dataset to S3 bucket (overwrites if already exists)."""
    try:
        s3.head_object(Bucket=BUCKET_NAME, Key=S3_KEY)
        logger.warning(f"⚠️ File already exists in S3: s3://{BUCKET_NAME}/{S3_KEY} (will overwrite)")
    except botocore.exceptions.ClientError as e:
        if e.response["Error"]["Code"] == "404":
            logger.info("ℹ️ File does not exist in S3, safe to upload.")
        else:
            raise

    logger.info(f"⬆️ Uploading {file_path} → s3://{BUCKET_NAME}/{S3_KEY}")
    s3.upload_file(file_path, BUCKET_NAME, S3_KEY)
    logger.info(f"✅ Upload complete: s3://{BUCKET_NAME}/{S3_KEY}")


def verify_s3_upload():
    """Verify uploaded file exists in S3."""
    logger.info("🔎 Verifying S3 upload...")
    response = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix="raw/")

    if "Contents" not in response:
        logger.error("❌ No files found under prefix 'raw/'.")
        return

    for obj in response["Contents"]:
        logger.info(f"📂 Found in S3: {obj['Key']} ({obj['Size']} bytes)")


def copy_to_local(file_path):
    """Copy dataset locally for downstream ETL."""
    if not os.path.exists(LOCAL_DATASET_DIR):
        os.makedirs(LOCAL_DATASET_DIR)
        logger.info(f"📂 Created local dataset folder: {LOCAL_DATASET_DIR}")

    dest_path = os.path.join(LOCAL_DATASET_DIR, os.path.basename(LOCAL_FILE_NAME))
    shutil.copy(file_path, dest_path)
    logger.info(f"✅ Copied dataset to local folder: {dest_path}")
    return dest_path


def verify_local_copy(dest_path):
    """Verify local dataset copy."""
    if os.path.exists(dest_path):
        size = os.path.getsize(dest_path)
        logger.info(f"✅ Local copy verified: {dest_path} ({size:,} bytes)")
    else:
        logger.error(f"❌ Local copy missing: {dest_path}")


def main_ingest():
    """End-to-end ingestion pipeline."""
    logger.info("🚀 Starting Kaggle → S3 → Local ingestion pipeline...")
    file_path = download_from_kaggle()
    upload_to_s3(file_path)
    dest_path = copy_to_local(file_path)
    verify_s3_upload()
    verify_local_copy(dest_path)
    logger.info("🎯 Pipeline completed successfully!")


# -----------------------
# Entry point
# -----------------------
if __name__ == "__main__":
    main_ingest()
