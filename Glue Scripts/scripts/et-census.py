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

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node Amazon S3
AmazonS3_node1733340279578 = glueContext.create_dynamic_frame.from_catalog(database="lastmile-db", table_name="us_census", transformation_ctx="AmazonS3_node1733340279578")

# Script generated for node Evaluate Data Quality
EvaluateDataQuality_node1733342595724_ruleset = """
    # Example rules: Completeness "colA" between 0.4 and 0.8, ColumnCount > 10
    Rules = [
        Completeness "label" = 1.0,
        ColumnValues "Year" in [ 2022, 2023 ] with threshold > 0.95,
        ColumnValues "Level" in [ "National", "State","County","Metropolitan" ] with threshold > 0.95,
        RowCount > 25,
        ColumnCount = 29
    ]
"""

EvaluateDataQuality_node1733342595724 = EvaluateDataQuality().process_rows(frame=AmazonS3_node1733340279578, ruleset=EvaluateDataQuality_node1733342595724_ruleset, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1733342595724", "enableDataQualityCloudWatchMetrics": True, "enableDataQualityResultsPublishing": True}, additional_options={"observations.scope":"ALL","performanceTuning.caching":"CACHE_NOTHING"})

# Script generated for node ruleOutcomes
ruleOutcomes_node1733342630223 = SelectFromCollection.apply(dfc=EvaluateDataQuality_node1733342595724, key="ruleOutcomes", transformation_ctx="ruleOutcomes_node1733342630223")

# Script generated for node originalData
originalData_node1733351318625 = SelectFromCollection.apply(dfc=EvaluateDataQuality_node1733342595724, key="originalData", transformation_ctx="originalData_node1733351318625")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=originalData_node1733351318625, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1733344159555", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1733351338824 = glueContext.write_dynamic_frame.from_options(frame=originalData_node1733351318625, connection_type="s3", format="glueparquet", connection_options={"path": "s3://last-mile-data-transformed/us-census-transformed/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="AmazonS3_node1733351338824")

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1733355964968 = glueContext.write_dynamic_frame.from_catalog(frame=originalData_node1733351318625, database="lastmile-db", table_name="us_census", transformation_ctx="AWSGlueDataCatalog_node1733355964968")

job.commit()