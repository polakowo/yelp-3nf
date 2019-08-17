from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql import Window, Row

# File paths
source_business_path = "s3://polakowo-yelp2/yelp_dataset/business.json"
source_addresses_path = "s3://polakowo-yelp2/staging_data/addresses"
target_businesses_path = "s3://polakowo-yelp2/staging_data/businesses"

business_df = spark.read.json(source_business_path)
addresses_df = spark.read.parquet(source_addresses_path)

# Take any other information and write it into businesses table.

businesses_df = business_df.join(addresses_df, (business_df["address"] == addresses_df["address"]) 
                 & (business_df["latitude"] == addresses_df["latitude"]) 
                 & (business_df["longitude"] == addresses_df["longitude"])
                 & (business_df["postal_code"] == addresses_df["postal_code"]), how="left")\
    .selectExpr("business_id", "address_id", "cast(is_open as boolean)", "name", "review_count", "stars")

businesses_df.write.parquet(target_businesses_path, mode="overwrite")