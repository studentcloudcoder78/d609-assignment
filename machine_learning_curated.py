import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
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

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1745979555851 = glueContext.create_dynamic_frame.from_catalog(database="d609", table_name="accelerometer_trusted", transformation_ctx="AccelerometerTrusted_node1745979555851")

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1746057240433 = glueContext.create_dynamic_frame.from_catalog(database="d609", table_name="step_trainer_trusted", transformation_ctx="StepTrainerTrusted_node1746057240433")

# Script generated for node SQL Query
SqlQuery3566 = '''
SELECT * from step_trainer_trusted join accelerometer_trusted on accelerometer_trusted.timestamp = step_trainer_trusted.sensorreadingtime
'''
SQLQuery_node1746226406598 = sparkSqlQuery(glueContext, query = SqlQuery3566, mapping = {"accelerometer_trusted":AccelerometerTrusted_node1745979555851, "step_trainer_trusted":StepTrainerTrusted_node1746057240433}, transformation_ctx = "SQLQuery_node1746226406598")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1746226406598, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1746057234838", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1746060811225 = glueContext.getSink(path="s3://michael-brown-d609/machine_learning_curated/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1746060811225")
AmazonS3_node1746060811225.setCatalogInfo(catalogDatabase="d609",catalogTableName="machine_learning_curated")
AmazonS3_node1746060811225.setFormat("json")
AmazonS3_node1746060811225.writeFrame(SQLQuery_node1746226406598)
job.commit()