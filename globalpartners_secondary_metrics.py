import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Initialize unified Spark Session
spark = SparkSession.builder \
    .appName("GlobalPartners_Unified_Metrics_Pipeline") \
    .getOrCreate()

# Base S3 Paths (Update with your exact bucket name)
s3_input_path = "s3://globalpartners-bucket/raw/"
s3_output_path = "s3://globalpartners-bucket/transformed/metrics/"

print("Loading raw Parquet data layers from S3...")
order_items = spark.read.parquet(f"{s3_input_path}order_items_raw/")
order_options = spark.read.parquet(f"{s3_input_path}order_item_options_raw/")

# Pre-calculate base transactional revenues to reuse across aggregations
items_base = order_items.withColumn("line_revenue", F.col("item_price") * F.col("item_quantity"))
options_base = order_options.withColumn("option_revenue", F.col("option_price") * F.col("option_quantity"))

# Establish a baseline date for sandbox recency calculations
max_date_row = items_base.select(F.max("creation_time_utc")).collect()
current_date_baseline = max_date_row[0][0]

# =====================================================================
# 1. METRIC: CUSTOMER SEGMENTATION, RFM & CHURN INDICATORS
# =====================================================================
print("Calculating Customer Segmentation & Churn Indicators...")
orders_summary = items_base.groupBy("user_id", "order_id", "creation_time_utc").agg(
    F.sum("line_revenue").alias("order_total_revenue")
)

user_window = Window.partitionBy("user_id").orderBy("creation_time_utc")
orders_with_lag = orders_summary.withColumn("prev_order_time", F.lag("creation_time_utc", 1).over(user_window))
orders_with_gap = orders_with_lag.withColumn("days_between_orders", F.datediff(F.col("creation_time_utc"), F.col("prev_order_time")))

customer_rfm = orders_with_gap.groupBy("user_id").agg(
    F.datediff(F.lit(current_date_baseline), F.max("creation_time_utc")).alias("recency_days"),
    F.countDistinct("order_id").alias("frequency_total"),
    F.sum("order_total_revenue").alias("monetary_total"),
    F.round(F.avg("days_between_orders"), 1).alias("avg_days_between_orders")
).na.fill(0, ["avg_days_between_orders"])

# Score rankings
r_win = Window.orderBy(F.col("recency_days").asc())
f_win = Window.orderBy(F.col("frequency_total").desc())
m_win = Window.orderBy(F.col("monetary_total").desc())

rfm_final = customer_rfm \
    .withColumn("r_score", F.when(F.percent_rank().over(r_win) <= 0.2, 5).when(F.percent_rank().over(r_win) <= 0.4, 4).when(F.percent_rank().over(r_win) <= 0.6, 3).when(F.percent_rank().over(r_win) <= 0.8, 2).otherwise(1)) \
    .withColumn("f_score", F.when(F.percent_rank().over(f_win) <= 0.2, 5).when(F.percent_rank().over(f_win) <= 0.4, 4).when(F.percent_rank().over(f_win) <= 0.6, 3).when(F.percent_rank().over(f_win) <= 0.8, 2).otherwise(1)) \
    .withColumn("m_score", F.when(F.percent_rank().over(m_win) <= 0.2, 5).when(F.percent_rank().over(m_win) <= 0.4, 4).when(F.percent_rank().over(m_win) <= 0.6, 3).when(F.percent_rank().over(m_win) <= 0.8, 2).otherwise(1)) \
    .withColumn("rfm_segment", F.when((F.col("r_score") >= 4) & (F.col("f_score") >= 4) & (F.col("m_score") >= 4), "VIP").when((F.col("f_score") <= 2) & (F.col("r_score") >= 4), "New Customer").when((F.col("r_score") <= 2) & (F.col("f_score") <= 2), "Churn Risk").otherwise("Regular Customer")) \
    .withColumn("churn_indicator", F.when(F.col("recency_days") > 45, "At Risk").otherwise("Active")) \
    .select("user_id", "recency_days", "frequency_total", F.round("monetary_total", 2).alias("monetary_total"), "avg_days_between_orders", "rfm_segment", "churn_indicator")

rfm_final.write.mode("overwrite").format("parquet").save(f"{s3_output_path}customer_segments/")

# =====================================================================
# 2. METRIC: SALES TRENDS MONITORING
# =====================================================================
print("Calculating Sales Trends Monitoring...")
sales_trends = items_base.groupBy(
    F.to_date("creation_time_utc").alias("sales_date"),
    F.year("creation_time_utc").alias("sales_year"),
    F.month("creation_time_utc").alias("sales_month"),
    F.weekofyear("creation_time_utc").alias("sales_week"),
    F.hour("creation_time_utc").alias("sales_hour"),
    "item_category"
).agg(
    F.round(F.sum("line_revenue"), 2).alias("total_revenue"),
    F.countDistinct("order_id").alias("total_orders")
)

sales_trends.write.mode("overwrite").format("parquet").save(f"{s3_output_path}sales_trends/")

# =====================================================================
# 3. METRIC: LOYALTY PROGRAM IMPACT
# =====================================================================
print("Calculating Loyalty Program Impact...")
loyalty_impact = items_base.groupBy("is_loyalty").agg(
    F.round(F.avg("line_revenue"), 2).alias("avg_order_spend"),
    F.countDistinct("order_id").alias("total_orders_placed"),
    F.countDistinct("user_id").alias("unique_customers"),
    F.round(F.sum("line_revenue") / F.countDistinct("user_id"), 2).alias("avg_lifetime_value_spend")
)

loyalty_impact.write.mode("overwrite").format("parquet").save(f"{s3_output_path}loyalty_impact/")

# =====================================================================
# 4. METRIC: TOP-PERFORMING LOCATIONS
# =====================================================================
print("Calculating Location Performance Metrics...")
location_performance = items_base.groupBy("restaurant_id").agg(
    F.round(F.sum("line_revenue"), 2).alias("total_revenue"),
    F.countDistinct("order_id").alias("total_orders"),
    F.round(F.avg("line_revenue"), 2).alias("average_order_value")
)

location_performance.write.mode("overwrite").format("parquet").save(f"{s3_output_path}location_performance/")

# =====================================================================
# 5. METRIC: PRICING & DISCOUNT EFFECTIVENESS
# =====================================================================
print("Calculating Pricing & Discount Effectiveness...")
discount_orders = options_base.filter(F.col("option_price") < 0).select("order_id").distinct().withColumn("is_discounted", F.lit(True))

orders_with_discount_flag = orders_summary.join(discount_orders, on="order_id", how="left").na.fill(False, ["is_discounted"])

discount_effectiveness = orders_with_discount_flag.groupBy("is_discounted").agg(
    F.count("order_id").alias("total_orders"),
    F.round(F.sum("order_total_revenue"), 2).alias("total_revenue_generated"),
    F.round(F.avg("order_total_revenue"), 2).alias("average_basket_value")
)

discount_effectiveness.write.mode("overwrite").format("parquet").save(f"{s3_output_path}discount_effectiveness/")

print("All 5 metrics processed and written as individual files successfully.")
