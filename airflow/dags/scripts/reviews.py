from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql import Window, Row

# File paths
source_review_path = "s3://polakowo-yelp2/yelp_dataset/review.json"
target_reviews_path = "s3://polakowo-yelp2/staging_data/reviews"

review_df = spark.read.json(source_review_path)

# The table can be used as-is, only minor transformations required.

# date field looks more like a timestamp
reviews_df = review_df.withColumnRenamed("date", "ts")\
    .withColumn("ts", F.to_timestamp("ts"))

reviews_df.write.parquet(target_reviews_path, mode="overwrite")