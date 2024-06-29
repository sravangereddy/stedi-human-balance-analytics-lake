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


args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node extarct_accelerometer_trusted
extarct_accelerometer_trusted_node1717846506001 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False}, connection_type="s3", format="json",
    connection_options={"paths": ["s3://my-second-bucket-sgereddy-learnings/accelerometer/trusted/"], "recurse": True},
    transformation_ctx="extarct_accelerometer_trusted_node1717846506001")

# Script generated for node extarct_customer_trusted
extarct_customer_trusted_node1717846432619 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False}, connection_type="s3", format="json",
    connection_options={"paths": ["s3://my-second-bucket-sgereddy-learnings/customer/trusted/"], "recurse": True},
    transformation_ctx="extarct_customer_trusted_node1717846432619")

# Script generated for node SQL Query
SqlQuery0 = '''
WITH DISTINT_CUST AS (
SELECT DISTINCT USER FROM ACCELEROMETER
)
SELECT CUSTOMER.* FROM CUSTOMER INNER JOIN DISTINT_CUST ON CUSTOMER.EMAIL = DISTINT_CUST.USER
'''
SQLQuery_node1717846575501 = sparkSqlQuery(glueContext, query=SqlQuery0,
                                           mapping={"customer": extarct_customer_trusted_node1717846432619,
                                                    "accelerometer": extarct_accelerometer_trusted_node1717846506001},
                                           transformation_ctx="SQLQuery_node1717846575501")

# Script generated for node Amazon S3
AmazonS3_node1717847000844 = glueContext.getSink(path="s3://my-second-bucket-sgereddy-learnings/customer/curated/",
                                                 connection_type="s3", updateBehavior="UPDATE_IN_DATABASE",
                                                 partitionKeys=[], enableUpdateCatalog=True,
                                                 transformation_ctx="AmazonS3_node1717847000844")
AmazonS3_node1717847000844.setCatalogInfo(catalogDatabase="my_stedi_project", catalogTableName="customer_curated")
AmazonS3_node1717847000844.setFormat("json")
AmazonS3_node1717847000844.writeFrame(SQLQuery_node1717846575501)
job.commit()
