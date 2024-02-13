import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglueml.transforms import EntityDetector
from pyspark.sql.types import StringType
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import *
from awsglue import DynamicFrame
import hashlib


def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Customer trusted
Customertrusted_node1707770338424 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_trusted",
    transformation_ctx="Customertrusted_node1707770338424",
)

# Script generated for node Accelerometer trusted
Accelerometertrusted_node1707836340370 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_trusted",
    transformation_ctx="Accelerometertrusted_node1707836340370",
)

# Script generated for node Accelerometer filter
SqlQuery0 = """
SELECT 
    DISTINCT cust.* 
FROM acc
JOIN cust ON acc.user = cust.email
"""
Accelerometerfilter_node1707836411455 = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={
        "cust": Customertrusted_node1707770338424,
        "acc": Accelerometertrusted_node1707836340370,
    },
    transformation_ctx="Accelerometerfilter_node1707836411455",
)

# Script generated for node Detect Sensitive Data
entity_detector = EntityDetector()
classified_map = entity_detector.classify_columns(
    Accelerometerfilter_node1707836411455,
    ["PERSON_NAME", "PHONE_NUMBER", "EMAIL"],
    1.0,
    0.1,
)


def pii_column_hash(original_cell_value):
    return hashlib.sha256(str(original_cell_value).encode()).hexdigest()


pii_column_hash_udf = udf(pii_column_hash, StringType())


def hashDf(df, keys):
    if not keys:
        return df
    df_to_hash = df.toDF()
    for key in keys:
        df_to_hash = df_to_hash.withColumn(key, pii_column_hash_udf(key))
    return DynamicFrame.fromDF(df_to_hash, glueContext, "updated_hashed_df")


DetectSensitiveData_node1707848141273 = hashDf(
    Accelerometerfilter_node1707836411455, list(classified_map.keys())
)

# Script generated for node Customer curated
Customercurated_node1707770663308 = glueContext.getSink(
    path="s3://udacity-davimlmart-data-lake/customer/curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="Customercurated_node1707770663308",
)
Customercurated_node1707770663308.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="customer_curated"
)
Customercurated_node1707770663308.setFormat("json")
Customercurated_node1707770663308.writeFrame(DetectSensitiveData_node1707848141273)
job.commit()
