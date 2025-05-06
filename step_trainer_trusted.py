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

# Script generated for node Data Source Customers
DataSourceCustomers_node1745979555851 = glueContext.create_dynamic_frame.from_catalog(database="d609", table_name="customers_curated", transformation_ctx="DataSourceCustomers_node1745979555851")

# Script generated for node Data Source Step Trainer
DataSourceStepTrainer_node1746057240433 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://michael-brown-d609/step_trainer/landing/"], "recurse": True}, transformation_ctx="DataSourceStepTrainer_node1746057240433")

# Script generated for node SQL Query
SqlQuery4026 = '''
select step_trainer_landing.sensorreadingtime, step_trainer_landing.serialnumber, step_trainer_landing.distancefromobject from step_trainer_landing join customers_curated on customers_curated.serialnumber = step_trainer_landing.serialnumber
'''
SQLQuery_node1746234770256 = sparkSqlQuery(glueContext, query = SqlQuery4026, mapping = {"customers_curated":DataSourceCustomers_node1745979555851, "step_trainer_landing":DataSourceStepTrainer_node1746057240433}, transformation_ctx = "SQLQuery_node1746234770256")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1746234770256, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1746057234838", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1746060811225 = glueContext.getSink(path="s3://michael-brown-d609/step_trainer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1746060811225")
AmazonS3_node1746060811225.setCatalogInfo(catalogDatabase="d609",catalogTableName="step_trainer_trusted")
AmazonS3_node1746060811225.setFormat("json")
AmazonS3_node1746060811225.writeFrame(SQLQuery_node1746234770256)
job.commit()