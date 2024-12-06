import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs

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

# Script generated for node Amazon S3
AmazonS3_node1733379183786 = glueContext.create_dynamic_frame.from_catalog(database="lastmile-db", table_name="capital_bikeshare", transformation_ctx="AmazonS3_node1733379183786")

# Script generated for node Evaluate Data Quality
EvaluateDataQuality_node1733379889070_ruleset = """
    # Example rules: Completeness "colA" between 0.4 and 0.8, ColumnCount > 10
    Rules = [
        Uniqueness "ride_id" > 0.5,
        Completeness "ride_id" between 0.9 and 1,
        IsComplete "started_at",
        IsComplete "ended_at"
    ]
"""

EvaluateDataQuality_node1733379889070 = EvaluateDataQuality().process_rows(frame=AmazonS3_node1733379183786, ruleset=EvaluateDataQuality_node1733379889070_ruleset, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1733379889070", "enableDataQualityCloudWatchMetrics": True, "enableDataQualityResultsPublishing": True}, additional_options={"performanceTuning.caching":"CACHE_NOTHING"})

# Script generated for node originalData
originalData_node1733383238998 = SelectFromCollection.apply(dfc=EvaluateDataQuality_node1733379889070, key="originalData", transformation_ctx="originalData_node1733383238998")

# Script generated for node ruleOutcomes
ruleOutcomes_node1733383240360 = SelectFromCollection.apply(dfc=EvaluateDataQuality_node1733379889070, key="ruleOutcomes", transformation_ctx="ruleOutcomes_node1733383240360")

# Script generated for node Drop Duplicates
DropDuplicates_node1733383329243 =  DynamicFrame.fromDF(originalData_node1733383238998.toDF().dropDuplicates(), glueContext, "DropDuplicates_node1733383329243")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=DropDuplicates_node1733383329243, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1733378903696", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1733383382160 = glueContext.write_dynamic_frame.from_options(frame=DropDuplicates_node1733383329243, connection_type="s3", format="csv", connection_options={"path": "s3://last-mile-data-transformed/capital-bikeshare-transformed/", "compression": "snappy", "partitionKeys": []}, transformation_ctx="AmazonS3_node1733383382160")

job.commit()