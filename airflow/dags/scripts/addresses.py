from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql import Window, Row

# File paths
source_business_path = "s3://polakowo-yelp2/yelp_dataset/business.json"
source_cities_path = "s3://polakowo-yelp2/staging_data/cities"
target_addresses_path = "s3://polakowo-yelp2/staging_data/addresses"

business_df = spark.read.json(source_business_path)
cities_df = spark.read.parquet(source_cities_path)

# Pull address information from business.json, but instead of city take newly created city_id.

addresses_df = business_df.selectExpr("address", "latitude", "longitude", "postal_code", "city", "state as state_code")\
    .join(cities_df.select("city", "state_code", "city_id"), ["city", "state_code"], how='left')\
    .drop("city", "state_code")\
    .distinct()\
    .withColumn("address_id", F.monotonically_increasing_id())

addresses_df.write.parquet(target_addresses_path, mode="overwrite")