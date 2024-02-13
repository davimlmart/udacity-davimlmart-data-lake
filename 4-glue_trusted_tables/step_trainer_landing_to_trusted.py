import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame


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

# Script generated for node Step trainer landing
Steptrainerlanding_node1707836340370 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="step_trainer_landing",
    transformation_ctx="Steptrainerlanding_node1707836340370",
)

# Script generated for node Research filter
SqlQuery0 = """
SELECT 
    step.* 
FROM step
JOIN cust ON step.serialnumber = cust.serialnumber
"""
Researchfilter_node1707836411455 = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={
        "cust": Customertrusted_node1707770338424,
        "step": Steptrainerlanding_node1707836340370,
    },
    transformation_ctx="Researchfilter_node1707836411455",
)

# Script generated for node Step trainer trusted
Steptrainertrusted_node1707770663308 = glueContext.getSink(
    path="s3://udacity-davimlmart-data-lake/step_trainer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="Steptrainertrusted_node1707770663308",
)
Steptrainertrusted_node1707770663308.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="step_trainer_trusted"
)
Steptrainertrusted_node1707770663308.setFormat("json")
Steptrainertrusted_node1707770663308.writeFrame(Researchfilter_node1707836411455)
job.commit()