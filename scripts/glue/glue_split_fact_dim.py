import sys
import boto3
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# -----------------------------
# Glue args
# -----------------------------
args = getResolvedOptions(sys.argv, ["JOB_NAME", "BUCKET_NAME", "INPUT_KEY"])
bucket = args["BUCKET_NAME"]
key = args["INPUT_KEY"]

# -----------------------------
# Spark + Glue context
# -----------------------------
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

print(f"🚀 Glue Split Fact-Dim Job started for s3://{bucket}/{key}")

# -----------------------------
# Load transformed parquet
# -----------------------------
df = spark.read.parquet(f"s3://{bucket}/{key}")
print(f"📊 Loaded {df.count()} rows from {key}")

# -----------------------------
# Dimension tables
# -----------------------------
dim_product = df.select("product_name", "category").dropDuplicates()
dim_date = df.select("crawl_year", "crawl_month").dropDuplicates()

# -----------------------------
# Fact table
# -----------------------------
fact_sales = df.select(
    "product_name",
    "current_discounted_price",
    "listed_price",
    "rating",
    "number_of_reviews",
    "discount_amount",
    "discount_flag",
    "rating_bucket",
    "crawl_year",
    "crawl_month"
)

# -----------------------------
# Write outputs to curated/
# -----------------------------
dim_product.write.mode("overwrite").parquet(f"s3://{bucket}/curated/dim_product/")
dim_date.write.mode("overwrite").parquet(f"s3://{bucket}/curated/dim_date/")
fact_sales.write.mode("overwrite").parquet(f"s3://{bucket}/curated/fact_sales/")

print("✅ Curated parquet files written to S3.")

# -----------------------------
# Trigger Lambda 3 (Load to Redshift)
# -----------------------------
try:
    lambda_client = boto3.client("lambda")
    lambda_client.invoke(
        FunctionName="lambda-load-redshift",  # Lambda 3 name
        InvocationType="Event"  # async → fire and forget
    )
    print("🚀 Successfully triggered Lambda 3 (Load to Redshift).")
except Exception as e:
    print(f"❌ Failed to trigger Lambda 3: {e}")

# -----------------------------
# Commit Glue job
# -----------------------------
job.commit()
print("🎯 Glue Split Fact-Dim Job completed successfully.")
