import boto3
import logging

# -----------------------
# Logging setup
# -----------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

# -----------------------
# AWS Clients
# -----------------------
events = boto3.client("events")
lambda_client = boto3.client("lambda")
glue = boto3.client("glue")
redshift = boto3.client("redshift-serverless")
s3 = boto3.client("s3")
iam = boto3.client("iam")
sts = boto3.client("sts")
account_id = sts.get_caller_identity()["Account"]

# -----------------------
# Config (aligned to actual pipeline)
# -----------------------
BUCKET_NAME = "dp-datawarehouse-solution-1"
EVENT_RULE = "trigger-lambda1-every-10min"   
LAMBDAS = [
    "lambda-trigger-glue",      # Lambda 1
    "lambda-split-fact-dim",    # Lambda 2
    "lambda-load-redshift"      # Lambda 3
]
WORKGROUP_NAME = "dw-workgroup"    
NAMESPACE_NAME = "dw-namespace"     
IAM_ROLES = [
    "glue-etl-role",
    "lambda-etl-role",
    "redshift-copy-role",
    "glue-crawler-role"
]

# -----------------------
# Cleanup Steps
# -----------------------
def delete_eventbridge():
    try:
        events.remove_targets(Rule=EVENT_RULE, Ids=["1"])
        events.delete_rule(Name=EVENT_RULE, Force=True)
        logger.info(f"✅ Deleted EventBridge rule {EVENT_RULE}")
    except Exception as e:
        logger.warning(f"⚠️ EventBridge cleanup skipped: {e}")

def delete_lambdas():
    for fn in LAMBDAS:
        try:
            lambda_client.delete_function(FunctionName=fn)
            logger.info(f"✅ Deleted Lambda {fn}")
        except Exception as e:
            logger.warning(f"⚠️ Lambda {fn} not deleted: {e}")

def delete_glue():
    # Delete both Glue jobs
    for job in ["glue-clean-transform", "glue-split-fact-dim"]:
        try:
            glue.delete_job(JobName=job)
            logger.info(f"✅ Deleted Glue job {job}")
        except Exception as e:
            logger.warning(f"⚠️ Glue job {job} not deleted: {e}")

    # Align names to those used in deploy_lambdas.py / lambda_3.py
    GLUE_CRAWLER = "product-data-crawler"
    GLUE_DATABASE = "product_db"

    try:
        glue.delete_crawler(Name=GLUE_CRAWLER)
        logger.info(f"✅ Deleted Glue crawler {GLUE_CRAWLER}")
    except Exception as e:
        logger.warning(f"⚠️ Glue crawler not deleted: {e}")

    try:
        glue.delete_database(Name=GLUE_DATABASE)
        logger.info(f"✅ Deleted Glue database {GLUE_DATABASE}")
    except Exception as e:
        logger.warning(f"⚠️ Glue database not deleted: {e}")

def delete_redshift():
    try:
        redshift.delete_workgroup(workgroupName=WORKGROUP_NAME)
        logger.info(f"✅ Deleted Redshift workgroup {WORKGROUP_NAME}")
    except Exception as e:
        logger.warning(f"⚠️ Redshift workgroup not deleted: {e}")

    try:
        redshift.delete_namespace(namespaceName=NAMESPACE_NAME)
        logger.info(f"✅ Deleted Redshift namespace {NAMESPACE_NAME}")
    except Exception as e:
        logger.warning(f"⚠️ Redshift namespace not deleted: {e}")

def delete_s3():
    try:
        # Delete all objects in bucket
        s3_resource = boto3.resource("s3")
        bucket = s3_resource.Bucket(BUCKET_NAME)
        bucket.objects.all().delete()
        bucket.object_versions.delete()
        bucket.delete()
        logger.info(f"✅ Deleted S3 bucket {BUCKET_NAME}")
    except Exception as e:
        logger.warning(f"⚠️ S3 bucket not deleted: {e}")

def delete_iam_roles():
    for role in IAM_ROLES:
        try:
            attached_policies = iam.list_attached_role_policies(RoleName=role)["AttachedPolicies"]
            for p in attached_policies:
                iam.detach_role_policy(RoleName=role, PolicyArn=p["PolicyArn"])
            iam.delete_role(RoleName=role)
            logger.info(f"✅ Deleted IAM role {role}")
        except Exception as e:
            logger.warning(f"⚠️ IAM role {role} not deleted: {e}")

# -----------------------
# Main
# -----------------------
def main():
    delete_eventbridge()
    delete_lambdas()
    delete_glue()
    delete_redshift()
    delete_s3()
    delete_iam_roles()
    logger.info("🎉 Cleanup completed successfully!")

if __name__ == "__main__":
    main()
