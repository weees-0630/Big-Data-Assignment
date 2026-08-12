# 1. Create Spark Script
nano spark_analysis.py

# 2. paste the below script into the py file

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql import Row
import time

# ==========================================================
# Create Spark Session
# ==========================================================

spark = SparkSession.builder \
    .appName("VideoGameAnalytics") \
    .getOrCreate()

print("Spark Session Created")

# ==========================================================
# Load Dataset from S3
# ==========================================================

df = spark.read.csv(
    "s3://ist3134-video-game-analysis/videogames_processed.csv",
    header=True,
    inferSchema=True
)

print("Dataset Loaded Successfully")

# ==========================================================
# Dataset Information
# ==========================================================

print("\nTotal Rows:")
print(df.count())

print("\nTotal Columns:")
print(len(df.columns))

# ==========================================================
# Data Cleaning
# ==========================================================

clean_df = df.filter(
    col("estimated_revenue_usd").isNotNull()
)

print("\nRows After Cleaning:")
print(clean_df.count())

# ==========================================================
# START TOTAL EXECUTION TIMER
# ==========================================================

analysis_start = time.time()

# ==========================================================
# Analysis 1
# Revenue by Genre
# ==========================================================

print("\n===================================")
print("ANALYSIS 1: REVENUE BY GENRE")
print("===================================")

genre_revenue = clean_df.groupBy(
    "genre"
).sum(
    "estimated_revenue_usd"
).orderBy(
    "sum(estimated_revenue_usd)",
    ascending=False
)

genre_revenue.collect()

genre_revenue.show(10, False)

# ==========================================================
# Analysis 2
# Revenue by Publisher
# ==========================================================

print("\n===================================")
print("ANALYSIS 2: REVENUE BY PUBLISHER")
print("===================================")

publisher_revenue = clean_df.groupBy(
    "publisher"
).sum(
    "estimated_revenue_usd"
).orderBy(
    "sum(estimated_revenue_usd)",
    ascending=False
)

publisher_revenue.collect()

publisher_revenue.show(10, False)

# ==========================================================
# Analysis 3
# Revenue by Platform
# ==========================================================

print("\n===================================")
print("ANALYSIS 3: REVENUE BY PLATFORM")
print("===================================")

platform_revenue = clean_df.groupBy(
    "platform"
).sum(
    "estimated_revenue_usd"
).orderBy(
    "sum(estimated_revenue_usd)",
    ascending=False
)

platform_revenue.collect()

platform_revenue.show(10, False)

# ==========================================================
# Analysis 4
# Price vs Revenue Correlation
# ==========================================================

print("\n===================================")
print("ANALYSIS 4: PRICE VS REVENUE")
print("===================================")

correlation = clean_df.stat.corr(
    "current_price_usd",
    "estimated_revenue_usd"
)

print("Correlation Value:", correlation)

# ==========================================================
# END TOTAL EXECUTION TIMER
# ==========================================================

analysis_end = time.time()

total_spark_time = (
    analysis_end - analysis_start
)

print("\n===================================")
print("TOTAL SPARK EXECUTION TIME")
print("===================================")

print(total_spark_time)

# ==========================================================
# Save Execution Time
# ==========================================================

time_df = spark.createDataFrame([
    Row(
        execution_time_seconds=total_spark_time
    )
])

time_df.write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(
        "s3://ist3134-video-game-analysis/results/execution_time"
    )

# ==========================================================
# Save Correlation
# ==========================================================

corr_df = spark.createDataFrame([
    Row(
        correlation=correlation
    )
])

corr_df.write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(
        "s3://ist3134-video-game-analysis/results/correlation"
    )

# ==========================================================
# Save Genre Results
# ==========================================================

genre_revenue.write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(
        "s3://ist3134-video-game-analysis/results/genre"
    )

# ==========================================================
# Save Publisher Results
# ==========================================================

publisher_revenue.write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(
        "s3://ist3134-video-game-analysis/results/publisher"
    )

# ==========================================================
# Save Platform Results
# ==========================================================

platform_revenue.write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(
        "s3://ist3134-video-game-analysis/results/platform"
    )

print("\nResults Saved Successfully")

# ==========================================================
# Stop Spark Session
# ==========================================================

spark.stop()

print("\nAnalysis Completed Successfully")

# 3. Upload Script to S3
aws s3 cp spark_analysis.py \
s3://ist3134-video-game-analysis/scripts/

# 4. Submit Spark Job to EMR
aws emr add-steps \
--cluster-id j-9DWONERMO4RB \
--steps Type=Spark,Name="VideoGameAnalysis",ActionOnFailure=CONTINUE,Args=[s3://ist3134-video-game-analysis/scripts/spark_analysis.py]

# 5. Monitor Job
aws emr describe-step \
--cluster-id j-9DWONERMO4RB \
--step-id s-0162770351LE7RJ57VIJ

# 6. Check output
aws s3 ls s3://ist3134-video-game-analysis/results/

# 7. View the results of genre
#Check:
aws s3 ls s3://ist3134-video-game-analysis/results/genre/
#Download:
aws s3 cp \
s3://ist3134-video-game-analysis/results/genre/part-00000-769c4403-f6f3-4f19-9619-dcdde5203e56-c000.csv \
genre_results.csv
#Display reults:
head -11 genre_results.csv | column -s, -t

# 8. View the results of Publisher
#Check:
aws s3 ls \
s3://ist3134-video-game-analysis/results/publisher/
#Download:
aws s3 cp \
s3://ist3134-video-game-analysis/results/publisher/part-00000-af56bdf6-cfb6-4279-9143-4589be1aa3c4-c000.csv \
publisher_results.csv
#Display:
head -11 publisher_results.csv | column -s, -t

# 9. View the results of Platform
#Check:
aws s3 ls \
s3://ist3134-video-game-analysis/results/platform/
#Download:
aws s3 cp \
s3://ist3134-video-game-analysis/results/platform/part-00000-0484d1d5-63da-4959-a69f-04caabd7989a-c000.csv \
platform_results.csv
#Display:
head -11 platform_results.csv | column -s, -t

# 10. View the results of Price vs Correlation
#Check:
aws s3 ls \
s3://ist3134-video-game-analysis/results/correlation/
#Download:
aws s3 cp \
s3://ist3134-video-game-analysis/results/correlation/part-00003-ac5a2b96-4d67-40b6-91e7-3ecd02b9502e-c000.csv \
correlation.csv
#Display:
cat correlation.csv

# 11. View the execution time
#Check:
aws s3 ls \
s3://ist3134-video-game-analysis/results/execution_time/
#Download:
aws s3 cp \
s3://ist3134-video-game-analysis/results/execution_time/part-00000-0af9b52b-e32d-48d2-9dc2-9013a507282a-c000.csv \
spark_time.csv
aws s3 cp \
s3://ist3134-video-game-analysis/results/execution_time/part-00003-0af9b52b-e32d-48d2-9dc2-9013a507282a-c000.csv \
spark_time.csv
#Display:
cat spark_time.csv


