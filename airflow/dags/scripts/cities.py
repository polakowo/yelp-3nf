from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql import Window, Row

# File paths
source_business_path = "s3://polakowo-yelp2/yelp_dataset/business.json"
source_demo_path = "s3://polakowo-yelp2/demo_dataset/us-cities-demographics.json"
target_cities_path = "s3://polakowo-yelp2/staging_data/cities"

business_df = spark.read.json(source_business_path)

# Take city and state_code from the business.json and enrich them with demographics data.

################
# Demographics #
################

demo_df = spark.read.json(source_demo_path)

# Each JSON object here seems to describe (1) the demographics of the city 
# and (2) the number of people belonging to some race (race and count fields). 
# Since each record is unique by city, state_code and race fields, while other 
# demographic fields are unique by only city and state_code (which means lots 
# of redundancy), we need to transform race column into columns corresponding 
# to each of its values via pivot function.

def prepare_race(x):
    # We want to make each race a stand-alone column, thus each race value needs a proper naming
    return x.replace(" ", "_").replace("-", "_").lower()
    
prepare_race_udf = F.udf(prepare_race, T.StringType())

# Group by all columns except race and count and convert race rows into columns
demo_df = demo_df.select("fields.*")\
    .withColumn("race", prepare_race_udf("race"))
demo_df = demo_df.groupby(*set(demo_df.schema.names).difference(set(["race", "count"])))\
    .pivot('race')\
    .max('count')
# Columns have a different order every now and then?
demo_df = demo_df.select(*sorted(demo_df.columns))

##########
# Cities #
##########

# Merge city data with demographics data
cities_df = business_df.selectExpr("city", "state as state_code")\
    .distinct()\
    .join(demo_df, ["city", "state_code"], how="left")\
    .withColumn("city_id", F.monotonically_increasing_id())

cities_df.write.parquet(target_cities_path, mode="overwrite")