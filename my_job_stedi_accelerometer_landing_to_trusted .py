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

# Script generated for node extract_accelerometer_landing
extract_accelerometer_landing_node1717846086317 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False}, connection_type="s3", format="json",
    connection_options={"paths": ["s3://my-second-bucket-sgereddy-learnings/accelerometer/landing/"], "recurse": True},
    transformation_ctx="extract_accelerometer_landing_node1717846086317")

# Script generated for node extarct_customer_trusted
extarct_customer_trusted_node1719670527091 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False}, connection_type="s3", format="json",
    connection_options={"paths": ["s3://my-second-bucket-sgereddy-learnings/customer/trusted/"], "recurse": True},
    transformation_ctx="extarct_customer_trusted_node1719670527091")

# Script generated for node transform_accelerometer_trusted
SqlQuery0 = '''
SELECT * FROM ACCELEROMETER INNER JOIN CUSTOMER ON ACCELEROMETER.USER = CUSTOMER.EMAIL
'''
transform_accelerometer_trusted_node1717845272901 = sparkSqlQuery(glueContext, query=SqlQuery0, mapping={
    "accelerometer": extract_accelerometer_landing_node1717846086317,
    "customer": extarct_customer_trusted_node1719670527091},
                                                                  transformation_ctx="transform_accelerometer_trusted_node1717845272901")

# Script generated for node load_accelerometer_trusted
load_accelerometer_trusted_node1717845486145 = glueContext.write_dynamic_frame.from_options(
    frame=transform_accelerometer_trusted_node1717845272901, connection_type="s3", format="json",
    connection_options={"path": "s3://my-second-bucket-sgereddy-learnings/accelerometer/trusted/", "partitionKeys": []},
    transformation_ctx="load_accelerometer_trusted_node1717845486145")

job.commit()
