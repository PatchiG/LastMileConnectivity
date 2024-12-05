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
AmazonS3_node1733383772275 = glueContext.create_dynamic_frame.from_catalog(database="lastmile-db", table_name="bus_wmata", transformation_ctx="AmazonS3_node1733383772275")

# Script generated for node Evaluate Data Quality
EvaluateDataQuality_node1733383787590_ruleset = """
    Rules = [
        # Completeness Checks
        IsComplete "route",                        # Ensures `route` is not null
        IsComplete "average of weekday",           # Ensures `average of weekday` is not null
        IsComplete "average of saturday",          # Ensures `average of saturday` is not null
        IsComplete "average of sunday",            # Ensures `average of sunday` is not null
        IsComplete "average ridership",            # Ensures `average ridership` is not null

        # Uniqueness Checks
        IsUnique "route",                          # Ensures `route` is unique across records

        # Numeric Type Validation (Double)
        ColumnDataType "average of weekday" = "double",    # Validates `average of weekday` is of type double
        ColumnDataType "average of saturday" = "double",   # Validates `average of saturday` is of type double
        ColumnDataType "average of sunday" = "double",     # Validates `average of sunday` is of type double
        ColumnDataType "average ridership" = "double",     # Validates `average ridership` is of type double

        # Column Count Validation
        ColumnCount = 5.0                     # Ensures dataset has exactly 5 columns
    ]

"""

EvaluateDataQuality_node1733383787590 = EvaluateDataQuality().process_rows(frame=AmazonS3_node1733383772275, ruleset=EvaluateDataQuality_node1733383787590_ruleset, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1733383787590", "enableDataQualityCloudWatchMetrics": True, "enableDataQualityResultsPublishing": True}, additional_options={"performanceTuning.caching":"CACHE_NOTHING"})

# Script generated for node originalData
originalData_node1733383853684 = SelectFromCollection.apply(dfc=EvaluateDataQuality_node1733383787590, key="originalData", transformation_ctx="originalData_node1733383853684")

# Script generated for node ruleOutcomes
ruleOutcomes_node1733383854306 = SelectFromCollection.apply(dfc=EvaluateDataQuality_node1733383787590, key="ruleOutcomes", transformation_ctx="ruleOutcomes_node1733383854306")

# Script generated for node Drop Duplicates
DropDuplicates_node1733384035916 =  DynamicFrame.fromDF(originalData_node1733383853684.toDF().dropDuplicates(), glueContext, "DropDuplicates_node1733384035916")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=DropDuplicates_node1733384035916, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1733383746579", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1733384171324 = glueContext.write_dynamic_frame.from_options(frame=DropDuplicates_node1733384035916, connection_type="s3", format="glueparquet", connection_options={"path": "s3://last-mile-data-transformed/wmata-bus-transformed/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="AmazonS3_node1733384171324")

job.commit()