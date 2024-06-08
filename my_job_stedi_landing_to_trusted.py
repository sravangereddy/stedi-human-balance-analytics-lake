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

# Script generated for node extract_customer_landing
extract_customer_landing_node1717846010819 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False}, connection_type="s3", format="json",
    connection_options={"paths": ["s3://my-second-bucket-sgereddy-learnings/customer/landing/"], "recurse": True},
    transformation_ctx="extract_customer_landing_node1717846010819")

# Script generated for node extract_accelerometer_landing
extract_accelerometer_landing_node1717846086317 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False}, connection_type="s3", format="json",
    connection_options={"paths": ["s3://my-second-bucket-sgereddy-learnings/accelerometer/landing/"], "recurse": True},
    transformation_ctx="extract_accelerometer_landing_node1717846086317")

# Script generated for node transfom_filter_customers
SqlQuery3631 = '''
SELECT * FROM CUSTOMER WHERE SHAREWITHRESEARCHASOFDATE!=0
'''
transfom_filter_customers_node1717844986783 = sparkSqlQuery(glueContext, query=SqlQuery3631, mapping={
    "customer": extract_customer_landing_node1717846010819},
     transformation_ctx="transfom_filter_customers_node1717844986783")

# Script generated for node transform_accelerometer_trusted
SqlQuery3632 = '''
SELECT * FROM ACCELEROMETER INNER JOIN CUSTOMER ON ACCELEROMETER.USER = CUSTOMER.EMAIL
'''
transform_accelerometer_trusted_node1717845272901 = sparkSqlQuery(glueContext, query=SqlQuery3632, mapping={
    "customer": transfom_filter_customers_node1717844986783,
    "accelerometer": extract_accelerometer_landing_node1717846086317},
     transformation_ctx="transform_accelerometer_trusted_node1717845272901")

# Script generated for node load_customer_trusted
load_customer_trusted_node1717844207005 = glueContext.write_dynamic_frame.from_options(
    frame=transfom_filter_customers_node1717844986783, connection_type="s3", format="json",
    connection_options={"path": "s3://my-second-bucket-sgereddy-learnings/customer/trusted/", "partitionKeys": []},
    transformation_ctx="load_customer_trusted_node1717844207005")

# Script generated for node load_accelerometer_trusted
load_accelerometer_trusted_node1717845486145 = glueContext.write_dynamic_frame.from_options(
    frame=transform_accelerometer_trusted_node1717845272901, connection_type="s3", format="json",
    connection_options={"path": "s3://my-second-bucket-sgereddy-learnings/accelerometer/trusted/", "partitionKeys": []},
    transformation_ctx="load_accelerometer_trusted_node1717845486145")

job.commit()
