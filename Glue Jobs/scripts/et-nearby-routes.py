import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Amazon S3
AmazonS3_node1733362711616 = glueContext.create_dynamic_frame.from_catalog(database="lastmile-db", table_name="nearby_routes", transformation_ctx="AmazonS3_node1733362711616")

# Script generated for node Evaluate Data Quality
EvaluateDataQuality_node1733363219594_ruleset = """
    # Example rules: Completeness "colA" between 0.4 and 0.8, ColumnCount > 10
    Rules = [
         IsComplete "route_short_name",
        IsComplete "location",
        IsComplete "global_route_id",
        IsComplete "route_long_name",
        IsComplete "route_type",
        IsComplete "network_name",
        IsComplete "latitudes",
        IsComplete "longitudes",

        # Uniqueness Check
        IsUnique "global_route_id",

        # Data Type Validation
        IsType "route_short_name" "string",
        IsType "location" "string",
        IsType "global_route_id" "string",
        IsType "route_long_name" "string",
        IsType "route_type" "long",
        IsType "network_name" "string",
        IsType "latitudes" "double",
        IsType "longitudes" "double",

        # Valid Value Range Checks
        IsBetween "latitudes" -90.0 90.0,
        IsBetween "longitudes" -180.0 180.0,

        # Consistency Checks
        MatchesRegex "route_short_name" "[a-zA-Z0-9]+",
        MatchesRegex "global_route_id" "[A-Z]+:[0-9]+",

        # Validity Checks
        IsIn "route_type" [0, 1, 2, 3, 4, 5], # Assume 0-5 are valid route types
        IsIn "network_name" ["TheBus", "OmniRide", "MARC","CUE","Metrobus","Fairfax Connector","Metrorail","ART","DASH","MTA Commuter Bus","Ride On","RTA of Central Maryland"]
    ]
"""

EvaluateDataQuality_node1733363219594 = EvaluateDataQuality().process_rows(frame=AmazonS3_node1733362711616, ruleset=EvaluateDataQuality_node1733363219594_ruleset, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1733363219594", "enableDataQualityCloudWatchMetrics": True, "enableDataQualityResultsPublishing": True}, additional_options={"performanceTuning.caching":"CACHE_NOTHING"})

# Script generated for node ruleOutcomes
ruleOutcomes_node1733363728419 = SelectFromCollection.apply(dfc=EvaluateDataQuality_node1733363219594, key="ruleOutcomes", transformation_ctx="ruleOutcomes_node1733363728419")

job.commit()