import boto3
import logging
from botocore.exceptions import ClientError

# -----------------------
# logging setup
# -----------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

# -----------------------
# config
# -----------------------
region = "us-east-1"   # change if needed
BUCKET_NAME = "dp-datawarehouse-solution-1"  # must be unique globally

s3 = boto3.client("s3", region_name=region)

def create_bucket():
    try:
        s3.head_bucket(Bucket=BUCKET_NAME)
        logger.info(f"ℹ️ Bucket already exists: {BUCKET_NAME}")
    except ClientError:
        try:
            if region == "us-east-1":
                s3.create_bucket(Bucket=BUCKET_NAME)
            else:
                s3.create_bucket(
                    Bucket=BUCKET_NAME,
                    CreateBucketConfiguration={"LocationConstraint": region}
                )
            logger.info(f"✅ Created bucket: {BUCKET_NAME}")
        except Exception as e:
            logger.error(f"❌ Error creating bucket {BUCKET_NAME}: {e}")

def enable_versioning_and_encryption():
    # Enable versioning
    s3.put_bucket_versioning(
        Bucket=BUCKET_NAME,
        VersioningConfiguration={"Status": "Enabled"}
    )
    logger.info("🔒 Versioning enabled")

    # Enable encryption
    s3.put_bucket_encryption(
        Bucket=BUCKET_NAME,
        ServerSideEncryptionConfiguration={
            "Rules": [
                {"ApplyServerSideEncryptionByDefault": {"SSEAlgorithm": "AES256"}}
            ]
        }
    )
    logger.info("🔑 Default encryption enabled")

def create_folders():
    folders = ["raw/", "transformed/", "curated/", "glue_scripts/"]
    for f in folders:
        s3.put_object(Bucket=BUCKET_NAME, Key=f)
        logger.info(f"📂 Created folder {f} in {BUCKET_NAME}")

if __name__ == "__main__":
    create_bucket()
    enable_versioning_and_encryption()
    create_folders()
    logger.info("✅ S3 bucket and folder setup complete")