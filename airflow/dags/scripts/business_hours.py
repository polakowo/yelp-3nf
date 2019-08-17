from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql import Window, Row

# File paths
source_business_path = "s3://polakowo-yelp2/yelp_dataset/business.json"
target_business_hours_path = "s3://polakowo-yelp2/staging_data/business_hours"

business_df = spark.read.json(source_business_path)
business_hours_df = business_df.select("business_id", "hours.*")

# To enable efficient querying based on business hours, for each day of week, 
# split the time range string into "from" and "to" integers.
# From
# Row(
#     business_id=u'QXAEGFB4oINsVuTFxEYKFQ', 
#     Monday=u'9:0-0:0', 
#     Tuesday=u'9:0-0:0', 
#     Wednesday=u'9:0-0:0',
#     Thursday=u'9:0-0:0', 
#     Friday=u'9:0-1:0', 
#     Saturday=u'9:0-1:0', 
#     Sunday=u'9:0-0:0'
# )
# To
# Row(
#     business_id=u'QXAEGFB4oINsVuTFxEYKFQ', 
#     Monday_from=900, 
#     Monday_to=0, 
#     Tuesday_from=900, 
#     Tuesday_to=0, 
#     Wednesday_from=900, 
#     Wednesday_to=0, 
#     Thursday_from=900, 
#     Thursday_to=0, 
#     Friday_from=900, 
#     Friday_to=100, 
#     Saturday_from=900, 
#     Saturday_to=100, 
#     Sunday_from=900, 
#     Sunday_to=0
# )

def parse_hours(x):
    # Take "9:0-0:0" (9am-00am) and transform it into {from: 900, to: 0}
    if x is None:
        return None
    convert_to_int = lambda x: int(x.split(':')[0]) * 100 + int(x.split(':')[1])
    return {
        "from": convert_to_int(x.split('-')[0]),
        "to": convert_to_int(x.split('-')[1])
    }
    
parse_hours_udf = F.udf(parse_hours, T.StructType([
    T.StructField('from', T.IntegerType(), nullable=True),
    T.StructField('to', T.IntegerType(), nullable=True)
]))

hour_attrs = [
    "Monday",
    "Tuesday",
    "Wednesday",
    "Thursday",
    "Friday",
    "Saturday",
    "Sunday",
]

for attr in hour_attrs:
    business_hours_df = business_hours_df.withColumn(attr, parse_hours_udf(attr))\
        .selectExpr("*", attr+".from as "+attr+"_from", attr+".to as "+attr+"_to")\
        .drop(attr)

business_hours_df.write.parquet(target_business_hours_path, mode="overwrite")