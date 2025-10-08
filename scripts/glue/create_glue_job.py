import boto3
import logging

# -----------------------
# logging setup
# -----------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

# -----------------------
# boto3 clients
# -----------------------
glue = boto3.client("glue")
sts = boto3.client("sts")
iam = boto3.client("iam")

# -----------------------
# config
# -----------------------
ROLE_NAME = "glue-etl-role"   # IAM role created earlier
BUCKET = "dp-datawarehouse-solution-1"  # <-- update if needed
TEMP_DIR = f"s3://{BUCKET}/temp/"

GLUE_JOBS = [
    {
        "Name": "glue-clean-transform",
        "ScriptPath": f"s3://{BUCKET}/glue_scripts/glue_clean_transform.py",
        "Description": "ETL job to clean and transform Amazon product sales data."
    },
    {
        "Name": "glue-split-fact-dim",
        "ScriptPath": f"s3://{BUCKET}/glue_scripts/glue_split_fact_dim.py",
        "Description": "ETL job to split transformed data into fact and dimension tables."
    }
]

# -----------------------
# helper functions
# -----------------------
def get_role_arn(role_name):
    """Dynamically fetch IAM Role ARN for current account"""
    account_id = sts.get_caller_identity()["Account"]
    return f"arn:aws:iam::{account_id}:role/{role_name}"

def verify_role_exists(role_name):
    """Ensure IAM role exists before using it"""
    try:
        iam.get_role(RoleName=role_name)
        return True
    except iam.exceptions.NoSuchEntityException:
        logger.error(f"❌ IAM Role '{role_name}' not found. Please create it first.")
        return False

def create_or_update_glue_job(job_name, script_path, description, role_arn):
    """Create Glue ETL job if not exists"""
    try:
        glue.get_job(JobName=job_name)
        logger.info(f"ℹ️ Glue job '{job_name}' already exists.")
    except glue.exceptions.EntityNotFoundException:
        logger.info(f"🛠 Creating Glue job '{job_name}' ...")
        glue.create_job(
            Name=job_name,
            Role=role_arn,
            Command={
                "Name": "glueetl",
                "ScriptLocation": script_path,
                "PythonVersion": "3"
            },
            DefaultArguments={
                "--TempDir": TEMP_DIR,
                "--job-language": "python"
            },
            GlueVersion="4.0",
            WorkerType="G.1X",
            NumberOfWorkers=2,
            Description=description
        )
        logger.info(f"✅ Glue job '{job_name}' created successfully.")

# -----------------------
# main
# -----------------------
if __name__ == "__main__":
    if not verify_role_exists(ROLE_NAME):
        exit(1)

    role_arn = get_role_arn(ROLE_NAME)

    for job in GLUE_JOBS:
        create_or_update_glue_job(
            job_name=job["Name"],
            script_path=job["ScriptPath"],
            description=job["Description"],
            role_arn=role_arn
        )
