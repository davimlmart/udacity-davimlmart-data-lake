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

# Script generated for node Step trainer trusted
Steptrainertrusted_node1707847165730 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="step_trainer_trusted",
    transformation_ctx="Steptrainertrusted_node1707847165730",
)

# Script generated for node Accelerometer trusted
Accelerometertrusted_node1707847165051 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_trusted",
    transformation_ctx="Accelerometertrusted_node1707847165051",
)

# Script generated for node Join step with accelerometer
SqlQuery0 = """
SELECT *
FROM acc
JOIN step ON acc.timestamp = step.sensorreadingtime;
"""
Joinstepwithaccelerometer_node1707847195509 = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={
        "step": Steptrainertrusted_node1707847165730,
        "acc": Accelerometertrusted_node1707847165051,
    },
    transformation_ctx="Joinstepwithaccelerometer_node1707847195509",
)

# Script generated for node Detect Sensitive Data
entity_detector = EntityDetector()
classified_map = entity_detector.classify_columns(
    Joinstepwithaccelerometer_node1707847195509,
    ["PERSON_NAME", "EMAIL", "PHONE_NUMBER"],
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


DetectSensitiveData_node1707849538080 = hashDf(
    Joinstepwithaccelerometer_node1707847195509, list(classified_map.keys())
)

# Script generated for node Machine learning curated
Machinelearningcurated_node1707847327636 = glueContext.getSink(
    path="s3://udacity-davimlmart-data-lake/machine_learning/curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="Machinelearningcurated_node1707847327636",
)
Machinelearningcurated_node1707847327636.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="machine_learning_curated"
)
Machinelearningcurated_node1707847327636.setFormat("json")
Machinelearningcurated_node1707847327636.writeFrame(
    DetectSensitiveData_node1707849538080
)
job.commit()
