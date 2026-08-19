# s3://globalpartners-bucket/transformed/

import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("GlobalPartners_CLV_Calculation") \
    .getOrCreate()

# 1a. DEFINE YOUR S3 SOURCE BUCKET PATH (Update this string with your actual bucket name)
s3_source_bucket_path = "s3://globalpartners-bucket/raw/"

# 1b. DEFINE YOUR S3 TARGET BUCKET PATH (Update this string with your actual bucket name)
s3_target_bucket_path = "s3://globalpartners-bucket/transformed/"

print("Reading Parquet data from S3...")
# Read the migrated Parquet datasets from S3
order_items_df = spark.read.parquet(f"{s3_source_bucket_path}order_items_raw/")
order_options_df = spark.read.parquet(f"{s3_source_bucket_path}order_item_options_raw/")

# 2. COMPUTE BASE REVENUE PER ITEM LINE
# Item revenue = unit price * quantity purchased
base_item_rev = order_items_df.withColumn(
    "item_line_revenue", 
    F.col("item_price") * F.col("item_quantity")
)

# 3. COMPUTE BASE REVENUE PER OPTION/MODIFIER LINE
# Option revenue = unit price * quantity added
base_option_rev = order_options_df.withColumn(
    "option_line_revenue", 
    F.col("option_price") * F.col("option_quantity")
)

# 4. AGGREGATE TOTAL REVENUE BY CUSTOMER
# Sum up core item spend per user
customer_item_spend = base_item_rev.groupBy("user_id").agg(
    F.sum("item_line_revenue").alias("total_item_spend")
)

# Sum up option modifications per user (requires joining options to items to get user_id)
item_user_map = order_items_df.select("lineitem_id", "user_id").distinct()
options_with_user = base_option_rev.join(item_user_map, on="lineitem_id", how="inner")

customer_option_spend = options_with_user.groupBy("user_id").agg(
    F.sum("option_line_revenue").alias("total_option_spend")
)

# Combine Item and Option spend to calculate final Customer Lifetime Value (CLV)
clv_base = customer_item_spend.join(customer_option_spend, on="user_id", how="left") \
    .na.fill(0, ["total_option_spend"]) \
    .withColumn("clv_revenue", F.col("total_item_spend") + F.col("total_option_spend")) \
    .select("user_id", F.round("clv_revenue", 2).alias("customer_lifetime_value"))

# 5. ASSIGN CLV TAGS BASED ON PERCENTILES
# Use Spark Windows to calculate a clean percentage rank across all customers
window_spec = Window.orderBy(F.col("customer_lifetime_value").desc())
clv_ranked = clv_base.withColumn("percent_rank", F.percent_rank().over(window_spec))

# Map percentiles: Top 20% -> High, Mid 60% -> Medium, Bottom 20% -> Low
clv_final = clv_ranked.withColumn(
    "clv_segment",
    F.when(F.col("percent_rank") <= 0.20, "High CLV")
     .when((F.col("percent_rank") > 0.20) & (F.col("percent_rank") <= 0.80), "Medium CLV")
     .otherwise("Low CLV")
).select("user_id", "customer_lifetime_value", "clv_segment")

# 6. SAVE RESULTS BACK TO S3
output_target_path = f"{s3_target_bucket_path}metrics/customer_lifetime_value/"
print(f"Saving final CLV metrics back to S3 at: {output_target_path}")

clv_final.write \
    .mode("overwrite") \
    .format("parquet") \
    .save(output_target_path)

print("Customer Lifetime Value aggregation complete.")
