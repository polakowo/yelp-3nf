from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql import Window, Row

# File paths
source_user_path = "s3://polakowo-yelp2/yelp_dataset/user.json"
friends_path = "s3://polakowo-yelp2/staging_data/friends"

user_df = spark.read.json(source_user_path)

# Basically the same procedure as elite to get the table of user relationships. 
# Can take some time.

friends_df = user_df.select("user_id", "friends")\
    .withColumn("friend_id", F.explode(F.split(F.col("friends"), ", ")))\
    .where("friend_id != '' and friend_id is not null")\
    .select(F.col("user_id"), F.col("friend_id"))\
    .distinct()

friends_df.write.parquet(friends_path, mode="overwrite")