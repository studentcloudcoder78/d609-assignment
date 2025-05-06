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
DataSourceCustomers_node1745979555851 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://michael-brown-d609/customer/landing/"], "recurse": True}, transformation_ctx="DataSourceCustomers_node1745979555851")

# Script generated for node Filter Customers
SqlQuery3378 = '''
select * from customer_landing where sharewithresearchasofdate is not null

'''
FilterCustomers_node1745982732988 = sparkSqlQuery(glueContext, query = SqlQuery3378, mapping = {"customer_landing":DataSourceCustomers_node1745979555851}, transformation_ctx = "FilterCustomers_node1745982732988")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=FilterCustomers_node1745982732988, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1745979191014", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1745983718334 = glueContext.getSink(path="s3://michael-brown-d609/customer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1745983718334")
AmazonS3_node1745983718334.setCatalogInfo(catalogDatabase="d609",catalogTableName="customer_trusted")
AmazonS3_node1745983718334.setFormat("json")
AmazonS3_node1745983718334.writeFrame(FilterCustomers_node1745982732988)
job.commit()