from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql import Window, Row

# File paths
source_tip_path = "s3://polakowo-yelp2/yelp_dataset/tip.json"
target_tips_path = "s3://polakowo-yelp2/staging_data/tips"

tip_df = spark.read.json(source_tip_path)

# Assign to each record a unique id for convenience.

tips_df = tip_df.withColumnRenamed("date", "ts")\
    .withColumn("ts", F.to_timestamp("ts"))\
    .withColumn("tip_id", F.monotonically_increasing_id())

tips_df.write.parquet(target_tips_path, mode="overwrite")