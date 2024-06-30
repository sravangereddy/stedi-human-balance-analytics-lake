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

# Script generated for node extract_step_trainer_trusted
extract_step_trainer_trusted_node1719772705073 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False}, connection_type="s3", format="json",
    connection_options={"paths": ["s3://my-second-bucket-sgereddy-learnings/step_trainer/trusted/"], "recurse": True},
    transformation_ctx="extract_step_trainer_trusted_node1719772705073")

# Script generated for node extract_accelerometer_trusted
extract_accelerometer_trusted_node1719772777454 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False}, connection_type="s3", format="json",
    connection_options={"paths": ["s3://my-second-bucket-sgereddy-learnings/accelerometer/trusted/"], "recurse": True},
    transformation_ctx="extract_accelerometer_trusted_node1719772777454")

# Script generated for node transform_machine_learning_curated
SqlQuery0 = '''
SELECT STEP.SENSORREADINGTIME, STEP.SERIALNUMBER, STEP.DISTANCEFROMOBJECT,ACCELEROMETER.USER,ACCELEROMETER.X,ACCELEROMETER.Y,ACCELEROMETER.Z FROM STEP INNER JOIN ACCELEROMETER ON STEP.SENSORREADINGTIME = ACCELEROMETER.TIMESTAMP
'''
transform_machine_learning_curated_node1719772845718 = sparkSqlQuery(glueContext, query=SqlQuery0, mapping={
    "accelerometer": extract_accelerometer_trusted_node1719772777454,
    "step": extract_step_trainer_trusted_node1719772705073},
                                                                     transformation_ctx="transform_machine_learning_curated_node1719772845718")

# Script generated for node load_machine_learning_trusted
load_machine_learning_trusted_node1719772879319 = glueContext.write_dynamic_frame.from_options(
    frame=transform_machine_learning_curated_node1719772845718, connection_type="s3", format="json",
    connection_options={"path": "s3://my-second-bucket-sgereddy-learnings/machine_learning/trusted/",
                        "compression": "snappy", "partitionKeys": []},
    transformation_ctx="load_machine_learning_trusted_node1719772879319")

job.commit()
