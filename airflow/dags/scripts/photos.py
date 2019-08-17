from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql import Window, Row

# File paths
source_photo_path = "s3://polakowo-yelp2/yelp_dataset/photo.json"
target_photos_path = "s3://polakowo-yelp2/staging_data/photos"

photos_df = spark.read.json(source_photo_path)

# Even if we do not store any photos, this table is useful for knowing how many and what kind of photos were taken.

photos_df.write.parquet(target_photos_path, mode="overwrite")