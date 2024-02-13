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

# Script generated for node Customer landing
Customerlanding_node1707763635615 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://udacity-davimlmart-data-lake/customer/landing/"],
        "recurse": True,
    },
    transformation_ctx="Customerlanding_node1707763635615",
)

# Script generated for node Research filter
SqlQuery0 = """
SELECT * FROM myDataSource
WHERE shareWithResearchAsOfDate != 0
"""
Researchfilter_node1707764524138 = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={"myDataSource": Customerlanding_node1707763635615},
    transformation_ctx="Researchfilter_node1707764524138",
)

# Script generated for node Customer trusted
Customertrusted_node1707763765737 = glueContext.getSink(
    path="s3://udacity-davimlmart-data-lake/customer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="Customertrusted_node1707763765737",
)
Customertrusted_node1707763765737.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="customer_trusted"
)
Customertrusted_node1707763765737.setFormat("json")
Customertrusted_node1707763765737.writeFrame(Researchfilter_node1707764524138)
job.commit()
