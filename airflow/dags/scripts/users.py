from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql import Window, Row

# File paths
source_user_path = "s3://polakowo-yelp2/yelp_dataset/user.json"
target_users_path = "s3://polakowo-yelp2/staging_data/users"

user_df = spark.read.json(source_user_path)

# Drop fields which will be outsourced and cast timestamp field
users_df = user_df.drop("elite", "friends")\
    .withColumn("yelping_since", F.to_timestamp("yelping_since"))

users_df.write.parquet(target_users_path, mode="overwrite")