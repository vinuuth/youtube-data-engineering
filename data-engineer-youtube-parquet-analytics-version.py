import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1720068236719 = glueContext.create_dynamic_frame.from_catalog(database="db_youtube_cleaned", table_name="raw_statistics", transformation_ctx="AWSGlueDataCatalog_node1720068236719")

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1720068274688 = glueContext.create_dynamic_frame.from_catalog(database="db_youtube_cleaned", table_name="cleaned_statistics_reference_data", transformation_ctx="AWSGlueDataCatalog_node1720068274688")

# Script generated for node Join
Join_node1720068815228 = Join.apply(frame1=AWSGlueDataCatalog_node1720068274688, frame2=AWSGlueDataCatalog_node1720068236719, keys1=["id"], keys2=["category_id"], transformation_ctx="Join_node1720068815228")

# Script generated for node Amazon S3
AmazonS3_node1720069062639 = glueContext.getSink(path="s3://data-engineer-youtube-analytics-useast1-dev", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=["region", "category_id"], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1720069062639")
AmazonS3_node1720069062639.setCatalogInfo(catalogDatabase="db_youtube_analytics",catalogTableName="final_analytics")
AmazonS3_node1720069062639.setFormat("glueparquet", compression="snappy")
AmazonS3_node1720069062639.writeFrame(Join_node1720068815228)
job.commit()