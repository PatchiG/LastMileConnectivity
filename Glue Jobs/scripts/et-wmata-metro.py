import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.gluetypes import *
from awsgluedq.transforms import EvaluateDataQuality
from awsglue.dynamicframe import DynamicFrame
from awsglue import DynamicFrame
from pyspark.sql import functions as SqlFuncs

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
AmazonS3_node1733384215983 = glueContext.create_dynamic_frame.from_catalog(database="lastmile-db", table_name="metro_wmata", transformation_ctx="AmazonS3_node1733384215983")

# Script generated for node Evaluate Data Quality
EvaluateDataQuality_node1733384251989_ruleset = """
    Rules = [
        # Completeness Checks
        IsComplete "station name",                   # Ensures `station name` is not null
        IsComplete "avg daily tapped entries",       # Ensures `avg daily tapped entries` is not null
        IsComplete "avg daily nontapped entries",    # Ensures `avg daily nontapped entries` is not null
        IsComplete "avg daily entries",              # Ensures `avg daily entries` is not null
        IsComplete "latitude",                       # Ensures `latitude` is not null
        IsComplete "longitude",                      # Ensures `longitude` is not null

        # Uniqueness Checks
        IsUnique "station name",                     # Ensures `station name` is unique across records

        # Numeric Type Validation (Long for entry counts, Double for latitude/longitude)
        ColumnDataType "avg daily tapped entries" = "long",        # Ensures `avg daily tapped entries` is of type long
        ColumnDataType "avg daily nontapped entries" = "long",     # Ensures `avg daily nontapped entries` is of type long
        ColumnDataType "avg daily entries" = "long",               # Ensures `avg daily entries` is of type long
        ColumnDataType "latitude" = "double",                      # Ensures `latitude` is of type double
        ColumnDataType "longitude" = "double",                     # Ensures `longitude` is of type double


        # Column Count Validation
        ColumnCount = 6.0                            # Ensures dataset has exactly 6 columns
    ]

"""

EvaluateDataQuality_node1733384251989 = EvaluateDataQuality().process_rows(frame=AmazonS3_node1733384215983, ruleset=EvaluateDataQuality_node1733384251989_ruleset, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1733384251989", "enableDataQualityCloudWatchMetrics": True, "enableDataQualityResultsPublishing": True}, additional_options={"performanceTuning.caching":"CACHE_NOTHING"})

# Script generated for node ruleOutcomes
ruleOutcomes_node1733384338989 = SelectFromCollection.apply(dfc=EvaluateDataQuality_node1733384251989, key="ruleOutcomes", transformation_ctx="ruleOutcomes_node1733384338989")

# Script generated for node originalData
originalData_node1733384338138 = SelectFromCollection.apply(dfc=EvaluateDataQuality_node1733384251989, key="originalData", transformation_ctx="originalData_node1733384338138")

# Script generated for node Drop Duplicates
DropDuplicates_node1733384584584 =  DynamicFrame.fromDF(originalData_node1733384338138.toDF().dropDuplicates(["station name"]), glueContext, "DropDuplicates_node1733384584584")

# Script generated for node Drop Null Fields
DropNullFields_node1733384587102 = drop_nulls(glueContext, frame=DropDuplicates_node1733384584584, nullStringSet={"", "null"}, nullIntegerSet={}, transformation_ctx="DropNullFields_node1733384587102")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=DropNullFields_node1733384587102, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1733383746579", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1733384615537 = glueContext.write_dynamic_frame.from_options(frame=DropNullFields_node1733384587102, connection_type="s3", format="glueparquet", connection_options={"path": "s3://last-mile-data-transformed/wmata-metro-transformed/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="AmazonS3_node1733384615537")

job.commit()