from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql import Window, Row

# File paths
source_user_path = "s3://polakowo-yelp2/yelp_dataset/user.json"
elite_years_path = "s3://polakowo-yelp2/staging_data/elite_years"

user_df = spark.read.json(source_user_path)

# The field elite is a comma-separated list of strings masked as a string. 
# Make a separate table out of it.

elite_years_df = user_df.select("user_id", "elite")\
    .withColumn("year", F.explode(F.split(F.col("elite"), ",")))\
    .where("year != '' and year is not null")\
    .select(F.col("user_id"), F.col("year").cast("integer"))

elite_years_df.write.parquet(elite_years_path, mode="overwrite")