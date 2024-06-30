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

# Script generated for node extract_step_trainer_landing
extract_step_trainer_landing_node1717847607065 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False}, connection_type="s3", format="json",
    connection_options={"paths": ["s3://my-second-bucket-sgereddy-learnings/step_trainer/landing/"], "recurse": True},
    transformation_ctx="extract_step_trainer_landing_node1717847607065")

# Script generated for node extarct_customer_trusted
extarct_customer_trusted_node1717846432619 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False}, connection_type="s3", format="json",
    connection_options={"paths": ["s3://my-second-bucket-sgereddy-learnings/customer/trusted/"], "recurse": True},
    transformation_ctx="extarct_customer_trusted_node1717846432619")

# Script generated for node SQL Query
SqlQuery1 = '''
WITH DISTINT_CUST AS (
SELECT DISTINCT USER FROM ACCELEROMETER
)
SELECT CUSTOMER.* FROM CUSTOMER INNER JOIN DISTINT_CUST ON CUSTOMER.EMAIL = DISTINT_CUST.USER
'''
SQLQuery_node1717846575501 = sparkSqlQuery(glueContext, query=SqlQuery1,
                                           mapping={"customer": extarct_customer_trusted_node1717846432619,
                                                    "accelerometer": extarct_accelerometer_trusted_node1717846506001},
                                           transformation_ctx="SQLQuery_node1717846575501")

# Script generated for node transform_step_trainer_trusted
SqlQuery0 = '''
SELECT STEP.* FROM STEP INNER JOIN CUSTOMER ON STEP.SERIALNUMBER = CUSTOMER.SERIALNUMBER
'''
transform_step_trainer_trusted_node1717847596198 = sparkSqlQuery(glueContext, query=SqlQuery0, mapping={
    "step": extract_step_trainer_landing_node1717847607065, "customer": SQLQuery_node1717846575501},
                                                                 transformation_ctx="transform_step_trainer_trusted_node1717847596198")

# Script generated for node Amazon S3
AmazonS3_node1719672102228 = glueContext.write_dynamic_frame.from_options(
    frame=transform_step_trainer_trusted_node1717847596198, connection_type="s3", format="json",
    connection_options={"path": "s3://my-second-bucket-sgereddy-learnings/step_trainer/trusted/",
                        "compression": "snappy", "partitionKeys": []}, transformation_ctx="AmazonS3_node1719672102228")

job.commit()
