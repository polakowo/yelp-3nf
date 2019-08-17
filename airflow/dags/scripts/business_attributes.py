from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql import Window, Row

# File paths
source_business_path = "s3://polakowo-yelp2/yelp_dataset/business.json"
target_business_attributes_path = "s3://polakowo-yelp2/staging_data/business_attributes"

business_df = spark.read.json(source_business_path)
business_attributes_df = business_df.select("business_id", "attributes.*")

# Unfold deep nested field attributes into a new table.

##################
# Parse booleans #
##################

# From
#     Row(AcceptsInsurance=None), 
#     Row(AcceptsInsurance=u'None'), 
#     Row(AcceptsInsurance=u'False'), 
#     Row(AcceptsInsurance=u'True')
# To
#     Row(AcceptsInsurance=None), 
#     Row(AcceptsInsurance=None), 
#     Row(AcceptsInsurance=False)
#     Row(AcceptsInsurance=True)

def parse_boolean(x):
    # Convert boolean strings to native boolean format
    if x is None or x == 'None':
        return None
    if x == 'True':
        return True
    if x == 'False':
        return False

parse_boolean_udf = F.udf(parse_boolean, T.BooleanType())

bool_attrs = [
    "AcceptsInsurance",
    "BYOB",
    "BikeParking", 
    "BusinessAcceptsBitcoin", 
    "BusinessAcceptsCreditCards",
    "ByAppointmentOnly", 
    "Caters", 
    "CoatCheck", 
    "Corkage", 
    "DogsAllowed",
    "DriveThru", 
    "GoodForDancing", 
    "GoodForKids",
    "HappyHour", 
    "HasTV",
    "Open24Hours", 
    "OutdoorSeating", 
    "RestaurantsCounterService", 
    "RestaurantsDelivery", 
    "RestaurantsGoodForGroups", 
    "RestaurantsReservations", 
    "RestaurantsTableService", 
    "RestaurantsTakeOut",
    "WheelchairAccessible"
]

for attr in bool_attrs:
    business_attributes_df = business_attributes_df.withColumn(attr, parse_boolean_udf(attr))

#################
# Parse strings #
#################

# From
#     Row(AgesAllowed=None), 
#     Row(AgesAllowed=u'None'), 
#     Row(AgesAllowed=u"u'18plus'"), 
#     Row(AgesAllowed=u"u'19plus'")
#     Row(AgesAllowed=u"u'21plus'"), 
#     Row(AgesAllowed=u"u'allages'"), 
# To
#     Row(AgesAllowed=None), 
#     Row(AgesAllowed=u'none'), 
#     Row(AgesAllowed=u'18plus')
#     Row(AgesAllowed=u'19plus'), 
#     Row(AgesAllowed=u'21plus'), 
#     Row(AgesAllowed=u'allages'), 



def parse_string(x):
    # Clean and standardize strings
    # Do not cast "None" into None since it has a special meaning
    if x is None or x == '':
        return None
    # Some strings are of format u"u'string'"
    return x.replace("u'", "").replace("'", "").lower()
    
parse_string_udf = F.udf(parse_string, T.StringType())

str_attrs = [
    "AgesAllowed", 
    "Alcohol",
    "BYOBCorkage",
    "NoiseLevel",
    "RestaurantsAttire",
    "Smoking",
    "WiFi",
]

for attr in str_attrs:
    business_attributes_df = business_attributes_df.withColumn(attr, parse_string_udf(attr))

##################
# Parse integers #
##################

# From
#     Row(RestaurantsPriceRange2=u'None'), 
#     Row(RestaurantsPriceRange2=None), 
#     Row(RestaurantsPriceRange2=u'1'), 
#     Row(RestaurantsPriceRange2=u'2')]
#     Row(RestaurantsPriceRange2=u'3'), 
#     Row(RestaurantsPriceRange2=u'4'), 
# To
#     Row(RestaurantsPriceRange2=None), 
#     Row(RestaurantsPriceRange2=None), 
#     Row(RestaurantsPriceRange2=1), 
#     Row(RestaurantsPriceRange2=2)
#     Row(RestaurantsPriceRange2=3), 
#     Row(RestaurantsPriceRange2=4), 

def parse_integer(x):
    # Convert integers masked as strings to native integer format
    if x is None or x == 'None':
        return None
    return int(x)
    
parse_integer_udf = F.udf(parse_integer, T.IntegerType())

int_attrs = [
    "RestaurantsPriceRange2",
]

for attr in int_attrs:
    business_attributes_df = business_attributes_df.withColumn(attr, parse_integer_udf(attr))

#######################
# Parse boolean dicts #
#######################

# From
# Row(
#     business_id=u'QXAEGFB4oINsVuTFxEYKFQ', 
#     Ambience=u"{'romantic': False, 'intimate': False, 'classy': False, 'hipster': False, 'divey': False, 'touristy': False, 'trendy': False, 'upscale': False, 'casual': True}"
# )
# To
# Row(
#     business_id=u'QXAEGFB4oINsVuTFxEYKFQ', 
#     Ambience_romantic=False, 
#     Ambience_intimate=False, 
#     Ambience_classy=False,
#     Ambience_hipster=False, 
#     Ambience_divey=False, 
#     Ambience_touristy=False, 
#     Ambience_trendy=False, 
#     Ambience_upscale=False, 
#     Ambience_casual=True
# )

import ast

def parse_boolean_dict(x):
    # Convert dicts masked as strings to string:boolean format
    if x is None or x == 'None' or x == '':
        return None
    return ast.literal_eval(x)

parse_boolean_dict_udf = F.udf(parse_boolean_dict, T.MapType(T.StringType(), T.BooleanType()))

bool_dict_attrs = [
    "Ambience",
    "BestNights",
    "BusinessParking",
    "DietaryRestrictions",
    "GoodForMeal",
    "HairSpecializesIn",
    "Music"
]

for attr in bool_dict_attrs:
    business_attributes_df = business_attributes_df.withColumn(attr, parse_boolean_dict_udf(attr))
    # Get all keys of the MapType
    # [Row(key=u'romantic'), Row(key=u'casual'), ...
    key_rows = business_attributes_df.select(F.explode(attr)).select("key").distinct().collect()
    # Convert each key into column (with proper name)
    exprs = ["{}['{}'] as {}".format(attr, row.key, attr+"_"+row.key.replace('-', '_')) for row in key_rows]
    business_attributes_df = business_attributes_df.selectExpr("*", *exprs).drop(attr)

business_attributes_df.write.parquet(target_business_attributes_path, mode="overwrite")