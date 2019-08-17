from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql import Window, Row

# File paths
source_business_path = "s3://polakowo-yelp2/yelp_dataset/business.json"
target_categories_path = "s3://polakowo-yelp2/staging_data/categories"
target_business_categories_path = "s3://polakowo-yelp2/staging_data/business_categories"

business_df = spark.read.json(source_business_path)

##############
# Categories #
##############

# First, create a list of unique categories and assign each of them an id.

import re
def parse_categories(categories):
    # Convert comma separated list of strings masked as a string into a native list type
    if categories is None:
        return []
    parsed = []
    # Some strings contain commas, so they have to be extracted beforehand
    require_attention = set(["Wills, Trusts, & Probates"])
    for s in require_attention:
        if categories.find(s) > -1:
            parsed.append(s)
            categories = categories.replace(s, "")
    return list(filter(None, parsed + re.split(r",\s*", categories)))
    
parse_categories_udf = F.udf(parse_categories, T.ArrayType(T.StringType()))
business_categories_df = business_df.select("business_id", "categories")\
    .withColumn("categories", parse_categories_udf("categories"))

# Convert the list of categories in each row into a set of rows
categories_df = business_categories_df.select(F.explode("categories").alias("category"))\
    .dropDuplicates()\
    .sort("category")\
    .withColumn("category_id", F.monotonically_increasing_id())

categories_df.write.parquet(target_categories_path, mode="overwrite")

#######################
# Business categories #
#######################

# For each record in business.json, convert list of categories in categories field into rows of pairs business_id-category_id.

import re
def zip_categories(business_id, categories):
    # For each value in categories, zip it with business_id to form a pair
    return list(zip([business_id] * len(categories), categories))
    
zip_categories_udf = F.udf(zip_categories, T.ArrayType(T.ArrayType(T.StringType())))

# Zip business_id's and categories and extract them into a new table called business_catagories
business_categories_df = business_categories_df.select(F.explode(zip_categories_udf("business_id", "categories")).alias("cols"))\
    .selectExpr("cols[0] as business_id", "cols[1] as category")\
    .dropDuplicates()
business_categories_df = business_categories_df.join(categories_df, business_categories_df["category"] == categories_df["category"], how="left")\
    .drop("category")

business_categories_df.write.parquet(target_business_categories_path, mode="overwrite")