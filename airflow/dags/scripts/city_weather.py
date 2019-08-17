"""Take city and state_code from the business.json and enrich them with demographics data."""

from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql import Window, Row

# File paths
source_cities_path = "s3://polakowo-yelp2/staging_data/cities"
source_city_attr_path = "s3://polakowo-yelp2/weather_dataset/city_attributes.csv"
source_weather_temp_path = "s3://polakowo-yelp2/weather_dataset/temperature.csv"
source_weather_desc_path = "s3://polakowo-yelp2/weather_dataset/weather_description.csv"
target_city_weather_path = "s3://polakowo-yelp2/staging_data/city_weather"

cities_df = spark.read.parquet(source_cities_path)

###################
# City attributes #
###################

# Get the names of US cities supported by this dataset and assign to each a city_id. 
# Requires reading the table cities.

city_attr_df = spark.read\
    .format('csv')\
    .option("header", "true")\
    .option("delimiter", ",")\
    .load(source_city_attr_path)

# We only want the list of US cities
cities = city_attr_df.where("Country = 'United States'")\
    .select("City")\
    .distinct()\
    .rdd.flatMap(lambda x: x)\
    .collect()

# Weather dataset doesn't provide us with the respective state codes though.
# How do we know whether "Phoenix" is in AZ or TX?
# The most appropriate solution is finding the biggest city.
# Let's find out which of those cities are referenced in Yelp dataset and relevant to us.
# Use Google or any other API.

weather_cities_df = [
    Row(city='Phoenix', state_code='AZ'), 
    Row(city='Dallas', state_code='TX'), 
    Row(city='Los Angeles', state_code='CA'), 
    Row(city='San Diego', state_code='CA'), 
    Row(city='Pittsburgh', state_code='PA'), 
    Row(city='Las Vegas', state_code='NV'), 
    Row(city='Seattle', state_code='WA'), 
    Row(city='New York', state_code='NY'), 
    Row(city='Charlotte', state_code='NC'), 
    Row(city='Denver', state_code='CO'), 
    Row(city='Boston', state_code='MA')
]
weather_cities_schema = T.StructType([
    T.StructField("city", T.StringType()),
    T.StructField("state_code", T.StringType())
])
weather_cities_df = spark.createDataFrame(weather_cities_df, schema=weather_cities_schema) 

# Join with the cities dataset to find matches
weather_cities_df = cities_df.join(weather_cities_df, ["city", "state_code"])\
    .select("city", "city_id")\
    .distinct()

################
# Temperatures #
################

# Read temperaturs recorded hourly, transform them into daily averages, and filter by our cities. 
# Also, cities are columns, so transform them into rows.

weather_temp_df = spark.read\
    .format('csv')\
    .option("header", "true")\
    .option("delimiter", ",")\
    .load(source_weather_temp_path)

# Extract date string from time string to be able to group by day
weather_temp_df = weather_temp_df.select("datetime", *cities)\
    .withColumn("date", F.substring("datetime", 0, 10))\
    .drop("datetime")

# For data quality check
import numpy as np
phoenix_rows = weather_temp_df.where("Phoenix is not null and date = '2012-10-01'").select("Phoenix").collect()
phoenix_mean_temp = np.mean([float(row.Phoenix) for row in phoenix_rows])

# To transform city columns into rows, transform each city individually and union all dataframes
temp_df = None
for city in cities:
    # Get average temperature in Fahrenheit for each day and city
    df = weather_temp_df.select("date", city)\
        .withColumnRenamed(city, "temperature")\
        .withColumn("temperature", F.col("temperature").cast("double"))\
        .withColumn("city", F.lit(city))\
        .groupBy("date", "city")\
        .agg(F.mean("temperature").alias("avg_temperature"))
    if temp_df is None:
        temp_df = df
    else:
        temp_df = temp_df.union(df)
weather_temp_df = temp_df

# Speed up further joins
weather_temp_df = weather_temp_df.repartition(1).cache()
weather_temp_df.count()

phoenix_mean_temp2 = weather_temp_df.where("city = 'Phoenix' and date = '2012-10-01'").collect()[0].avg_temperature
assert(phoenix_mean_temp == phoenix_mean_temp2)
# If we pass, the calculations are done correctly

########################
# Weather descriptions #
########################

# Read weather descriptions recorded hourly, pick the most frequent one on each day, and filter by our cities.
# The same as for temperatures, transform columns into rows.

weather_desc_df = spark.read\
    .format('csv')\
    .option("header", "true")\
    .option("delimiter", ",")\
    .load(source_weather_desc_path)

# Extract date string from time string to be able to group by day
weather_desc_df = weather_desc_df.select("datetime", *cities)\
    .withColumn("date", F.substring("datetime", 0, 10))\
    .drop("datetime")

# For data quality check
from collections import Counter
phoenix_rows = weather_desc_df.where("Phoenix is not null and date = '2012-12-10'").select("Phoenix").collect()
phoenix_most_common_weather = Counter([row.Phoenix for row in phoenix_rows]).most_common()[0][0]

# To transform city columns into rows, transform each city individually and union all dataframes
temp_df = None
for city in cities:
    # Get the most frequent description for each day and city
    window = Window.partitionBy("date", "city").orderBy(F.desc("count"))
    df = weather_desc_df.select("date", city)\
        .withColumnRenamed(city, "weather_description")\
        .withColumn("city", F.lit(city))\
        .groupBy("date", "city", "weather_description")\
        .count()\
        .withColumn("order", F.row_number().over(window))\
        .where(F.col("order") == 1)\
        .drop("count", "order")
    if temp_df is None:
        temp_df = df
    else:
        temp_df = temp_df.union(df)
weather_desc_df = temp_df

# Speed up further joins
weather_desc_df = weather_desc_df.repartition(1).cache()
weather_desc_df.count()

phoenix_most_common_weather2 = weather_desc_df.where("city = 'Phoenix' and date = '2012-12-10'").collect()[0].weather_description
assert(phoenix_most_common_weather == phoenix_most_common_weather2)
# If we pass, the calculations are done correctly

################
# City weather #
################

# What was the weather in the city when the particular review was posted?
# Join weather description with temperature, and keep only city ids which are present in Yelp.
city_weather_df = weather_temp_df.join(weather_desc_df, ["city", "date"])\
    .join(weather_cities_df, "city")\
    .drop("city")\
    .distinct()\
    .withColumn("date", F.to_date("date"))

city_weather_df.write.parquet(target_city_weather_path, mode="overwrite")