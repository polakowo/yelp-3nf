from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql import Window, Row

# File paths
source_checkin_path = "s3://polakowo-yelp2/yelp_dataset/checkin.json"
target_checkins_path = "s3://polakowo-yelp2/staging_data/checkins"

checkin_df = spark.read.json(source_checkin_path)

# Basically the same procedure as friends to get the table of pairs business_id:ts

checkins_df = checkin_df.selectExpr("business_id", "date as ts")\
    .withColumn("ts", F.explode(F.split(F.col("ts"), ", ")))\
    .where("ts != '' and ts is not null")\
    .withColumn("ts", F.to_timestamp("ts"))

checkins_df.write.parquet(target_checkins_path, mode="overwrite")