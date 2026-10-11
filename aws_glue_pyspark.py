
import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F

# Initialize AWS Glue and Spark
args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext.getOrCreate()
glue_context = GlueContext(sc)
spark = glue_context.spark_session

job = Job(glue_context)
job.init(args["JOB_NAME"], args)

INPUT_PATH = "s3://lab2-twitter-sc-825990809862-ap-southeast-1-an/input/twitter_combined.txt"

OUTPUT_PATH = (
    "s3://lab2-twitter-sc-825990809862-ap-southeast-1-an/"
    "spark-output/user-stats/"
)

# 1. Read the original dataset
lines = spark.read.text(INPUT_PATH)

# 2. Split each line into user IDs
parts = F.split(F.trim(F.col("value")), r"\s+")

edges = (
    lines
    .select(
        parts.getItem(0).alias("follower"),
        parts.getItem(1).alias("followee")
    )
    .filter(
        F.col("follower").isNotNull()
        & F.col("followee").isNotNull()
        & F.col("follower").rlike(r"^\d+$")
        & F.col("followee").rlike(r"^\d+$")
    )
    .dropDuplicates(["follower", "followee"])
)

# 3. Calculate follower counts (in-degree)
followers = (
    edges
    .groupBy(F.col("followee").alias("user_id"))
    .agg(F.count("*").alias("followers"))
)

# 4. Calculate followee counts (out-degree)
followees = (
    edges
    .groupBy(F.col("follower").alias("user_id"))
    .agg(F.count("*").alias("followees"))
)

# 5. Combine statistics for all users
user_stats = (
    followers
    .join(followees, "user_id", "full_outer")
    .fillna(0, subset=["followers", "followees"])
)

# 6. Save the results to Amazon S3
(
    user_stats.write
    .mode("overwrite")
    .parquet(OUTPUT_PATH)
)

# 7. Print the two required users for verification
user_stats.filter(
    F.col("user_id").isin("214328887", "107830991")
).orderBy("user_id").show(truncate=False)

job.commit()
