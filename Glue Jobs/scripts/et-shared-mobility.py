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
AmazonS3_node1733381435222 = glueContext.create_dynamic_frame.from_catalog(database="lastmile-db", table_name="shared_mobility", transformation_ctx="AmazonS3_node1733381435222")

# Script generated for node Evaluate Data Quality
EvaluateDataQuality_node1733381481140_ruleset = """
    Rules = [
        # Completeness Checks
        IsComplete "title",            # Ensures `title` is not null
        IsComplete "id",               # Ensures `id` is not null
        IsComplete "networkname",      # Ensures `networkname` is not null
        IsComplete "networkid",        # Ensures `networkid` is not null
        IsComplete "latitude",         # Ensures `latitude` is not null
        IsComplete "longitude",        # Ensures `longitude` is not null
        IsComplete "type",             # Ensures `type` is not null
        IsComplete "location",         # Ensures `location` is not null

        # Uniqueness Checks
        IsUnique "id",                 # Ensures `id` is unique across records

        # Data Type Validation
        
        ColumnDataType "networkid" = "long",
        ColumnDataType "latitude" = "double",
        ColumnDataType "longitude" = "double",
        ColumnDataType "bike_count" = "long",
        ColumnDataType "electric_bike_count" = "long",
        ColumnDataType "dock_count" = "long",

        # Column Count Validation
        ColumnCount between 10 and 20   # Ensures dataset has between 10 and 20 columns
    ]

"""

EvaluateDataQuality_node1733381481140 = EvaluateDataQuality().process_rows(frame=AmazonS3_node1733381435222, ruleset=EvaluateDataQuality_node1733381481140_ruleset, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1733381481140", "enableDataQualityCloudWatchMetrics": True, "enableDataQualityResultsPublishing": True}, additional_options={"performanceTuning.caching":"CACHE_NOTHING"})

# Script generated for node ruleOutcomes
ruleOutcomes_node1733381622381 = SelectFromCollection.apply(dfc=EvaluateDataQuality_node1733381481140, key="ruleOutcomes", transformation_ctx="ruleOutcomes_node1733381622381")

# Script generated for node originalData
originalData_node1733382475510 = SelectFromCollection.apply(dfc=EvaluateDataQuality_node1733381481140, key="originalData", transformation_ctx="originalData_node1733382475510")

# Script generated for node Drop Fields
DropFields_node1733382558975 = DropFields.apply(frame=originalData_node1733382475510, paths=["textcolor", "color"], transformation_ctx="DropFields_node1733382558975")

# Script generated for node Drop Duplicates
DropDuplicates_node1733382573255 =  DynamicFrame.fromDF(DropFields_node1733382558975.toDF().dropDuplicates(["id"]), glueContext, "DropDuplicates_node1733382573255")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=DropDuplicates_node1733382573255, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1733381410148", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1733382719634 = glueContext.write_dynamic_frame.from_options(frame=DropDuplicates_node1733382573255, connection_type="s3", format="glueparquet", connection_options={"path": "s3://last-mile-data-transformed", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="AmazonS3_node1733382719634")

job.commit()