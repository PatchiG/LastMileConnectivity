import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.gluetypes import *
from awsgluedq.transforms import EvaluateDataQuality
from awsglue import DynamicFrame

def _find_null_fields(ctx, schema, path, output, nullStringSet, nullIntegerSet, frame):
    if isinstance(schema, StructType):
        for field in schema:
            new_path = path + "." if path != "" else path
            output = _find_null_fields(ctx, field.dataType, new_path + field.name, output, nullStringSet, nullIntegerSet, frame)
    elif isinstance(schema, ArrayType):
        if isinstance(schema.elementType, StructType):
            output = _find_null_fields(ctx, schema.elementType, path, output, nullStringSet, nullIntegerSet, frame)
    elif isinstance(schema, NullType):
        output.append(path)
    else:
        x, distinct_set = frame.toDF(), set()
        for i in x.select(path).distinct().collect():
            distinct_ = i[path.split('.')[-1]]
            if isinstance(distinct_, list):
                distinct_set |= set([item.strip() if isinstance(item, str) else item for item in distinct_])
            elif isinstance(distinct_, str) :
                distinct_set.add(distinct_.strip())
            else:
                distinct_set.add(distinct_)
        if isinstance(schema, StringType):
            if distinct_set.issubset(nullStringSet):
                output.append(path)
        elif isinstance(schema, IntegerType) or isinstance(schema, LongType) or isinstance(schema, DoubleType):
            if distinct_set.issubset(nullIntegerSet):
                output.append(path)
    return output

def drop_nulls(glueContext, frame, nullStringSet, nullIntegerSet, transformation_ctx) -> DynamicFrame:
    nullColumns = _find_null_fields(frame.glue_ctx, frame.schema(), "", [], nullStringSet, nullIntegerSet, frame)
    return DropFields.apply(frame=frame, paths=nullColumns, transformation_ctx=transformation_ctx)

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
AmazonS3_node1733383236607 = glueContext.create_dynamic_frame.from_catalog(database="lastmile-db", table_name="nearby_stops", transformation_ctx="AmazonS3_node1733383236607")

# Script generated for node Evaluate Data Quality
EvaluateDataQuality_node1733383252498_ruleset = """
    Rules = [
        # Completeness Checks
        IsComplete "global_stop_id",                 # Ensures `global_stop_id` is not null
        IsComplete "location_name",                 # Ensures `location_name` is not null
        IsComplete "stop_lat",                      # Ensures `stop_lat` is not null
        IsComplete "stop_lon",                      # Ensures `stop_lon` is not null
        IsComplete "stop_name",                     # Ensures `stop_name` is not null

        # Uniqueness Checks
        IsUnique "global_stop_id",                  # Ensures `global_stop_id` is unique across records

        # Numeric Type Validation (Double/Long)
        ColumnDataType "distance" = "long",         # Validates `distance` is of type long
        ColumnDataType "location_type" = "long",    # Validates `location_type` is of type long
        ColumnDataType "route_type" = "long",       # Validates `route_type` is of type long
        ColumnDataType "stop_lat" = "double",       # Validates `stop_lat` is of type double
        ColumnDataType "stop_lon" = "double",       # Validates `stop_lon` is of type double
        ColumnDataType "wheelchair_boarding" = "long", # Validates `wheelchair_boarding` is of type long

        # Column Count Validation
        ColumnCount between 10 and 15               # Ensures dataset has between 10 and 15 columns
    ]

"""

EvaluateDataQuality_node1733383252498 = EvaluateDataQuality().process_rows(frame=AmazonS3_node1733383236607, ruleset=EvaluateDataQuality_node1733383252498_ruleset, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1733383252498", "enableDataQualityCloudWatchMetrics": True, "enableDataQualityResultsPublishing": True}, additional_options={"performanceTuning.caching":"CACHE_NOTHING"})

# Script generated for node ruleOutcomes
ruleOutcomes_node1733383388011 = SelectFromCollection.apply(dfc=EvaluateDataQuality_node1733383252498, key="ruleOutcomes", transformation_ctx="ruleOutcomes_node1733383388011")

# Script generated for node originalData
originalData_node1733383386925 = SelectFromCollection.apply(dfc=EvaluateDataQuality_node1733383252498, key="originalData", transformation_ctx="originalData_node1733383386925")

# Script generated for node Drop Fields
DropFields_node1733383490949 = DropFields.apply(frame=originalData_node1733383386925, paths=["parent_station_global_stop_id"], transformation_ctx="DropFields_node1733383490949")

# Script generated for node Drop Null Fields
DropNullFields_node1733383512837 = drop_nulls(glueContext, frame=DropFields_node1733383490949, nullStringSet={""}, nullIntegerSet={}, transformation_ctx="DropNullFields_node1733383512837")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=DropNullFields_node1733383512837, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1733381410148", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1733383531547 = glueContext.write_dynamic_frame.from_options(frame=DropNullFields_node1733383512837, connection_type="s3", format="glueparquet", connection_options={"path": "s3://last-mile-data-transformed/nearby-stops-transformed/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="AmazonS3_node1733383531547")

job.commit()