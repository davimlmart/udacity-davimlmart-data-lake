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

# Script generated for node Accelerometer landing
Accelerometerlanding_node1707836340370 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_landing",
    transformation_ctx="Accelerometerlanding_node1707836340370",
)

# Script generated for node Research filter
SqlQuery0 = """
SELECT 
    acc.* 
FROM acc
JOIN cust ON acc.user = cust.email
"""
Researchfilter_node1707836411455 = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={
        "cust": Customertrusted_node1707770338424,
        "acc": Accelerometerlanding_node1707836340370,
    },
    transformation_ctx="Researchfilter_node1707836411455",
)

# Script generated for node Accelerometer trusted
Accelerometertrusted_node1707770663308 = glueContext.getSink(
    path="s3://udacity-davimlmart-data-lake/accelerometer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="Accelerometertrusted_node1707770663308",
)
Accelerometertrusted_node1707770663308.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="accelerometer_trusted"
)
Accelerometertrusted_node1707770663308.setFormat("json")
Accelerometertrusted_node1707770663308.writeFrame(Researchfilter_node1707836411455)
job.commit()
