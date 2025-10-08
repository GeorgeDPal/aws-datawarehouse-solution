import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from awsglue.utils import getResolvedOptions
import os
import logging

# Initialize logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# -----------------------
# Glue Job boilerplate
# -----------------------
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init("glue-clean-transform", {})

# -----------------------
# S3 paths
# -----------------------
RAW_PATH = "s3://dp-datawarehouse-solution-1/raw/amazon_products_sales_data_uncleaned.csv"
TRANSFORMED_PATH = "s3://dp-datawarehouse-solution-1/transformed/"

# -----------------------
# Step 1: Read raw CSV
# -----------------------
df = (
    spark.read
         .option("header", True)
         .option("inferSchema", True)
         .option("mode", "PERMISSIVE")  # avoid hard failures on bad rows
         .csv(RAW_PATH)
)
# -----------------------
# Step 2: Column standardization
# -----------------------
df = df.toDF(*[c.strip().lower().replace(" ", "_").replace("/", "_") for c in df.columns])

# -----------------------
# Step 3: Cleaning
# -----------------------
df = df.select([F.trim(F.col(c)).alias(c) for c in df.columns])
df = df.dropDuplicates()
df = df.replace("", None)

drop_cols = ["product_url", "image_url"]
for col in drop_cols:
    if col in df.columns:
        df = df.drop(col)

# -----------------------
# Step 4: Data type conversions
# -----------------------
numeric_cols = ["current_discounted_price", "listed_price", "rating", "number_of_reviews"]
for col in numeric_cols:
    if col in df.columns:
        df = df.withColumn(col, F.regexp_replace(F.col(col), r"[^0-9.]", "").cast("double"))

if "collected_at" in df.columns:
    df = df.withColumn("collected_at", F.to_timestamp("collected_at"))
    df = df.withColumn("crawl_year", F.year("collected_at"))
    df = df.withColumn("crawl_month", F.month("collected_at"))

# -----------------------
# Step 5: Feature Engineering
# -----------------------
if "listed_price" in df.columns and "current_discounted_price" in df.columns:
    df = df.withColumn("discount_amount", F.col("listed_price") - F.col("current_discounted_price"))

if "discount_amount" in df.columns:
    df = df.withColumn("discount_flag", F.when(F.col("discount_amount") > 0, 1).otherwise(0))

if "rating" in df.columns:
    df = df.withColumn(
        "rating_bucket",
        F.when(F.col("rating") >= 4.5, "Excellent")
         .when(F.col("rating") >= 3.5, "Good")
         .when(F.col("rating") >= 2.5, "Average")
         .otherwise("Poor")
    )

if "about_product" in df.columns:
    df = df.withColumn("about_product_length", F.length("about_product"))

# -----------------------
# Step 6: Write Parquet (partitioned)
# -----------------------
try:
    if "crawl_year" in df.columns and "crawl_month" in df.columns:
        # enable dynamic partition overwrite to avoid wiping unrelated partitions
        spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
        df.write.mode("overwrite").option("partitionOverwriteMode", "dynamic").partitionBy("crawl_year", "crawl_month").parquet(TRANSFORMED_PATH)
        logger.info("Transformation complete with partitioning by crawl_year/crawl_month")
    else:
        df.write.mode("overwrite").parquet(TRANSFORMED_PATH)
        logger.info("Transformation complete without partitioning")
    job.commit()
except Exception as e:
    logger.exception("Job failed: %s", e)
    # Do not swallow error — re-raise so Glue marks job failed
    raise
