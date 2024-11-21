import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as F

# @params
args = getResolvedOptions(sys.argv, [
    'JOB_NAME', 
    'S3_PATH', 
    'REDSHIFT_TABLE_NAME', 
    'REDSHIFT_DATABASE_NAME',
    'DC_CONNECTION_NAME',
    'REDSHIFT_TEMP_DIR', 
    'GLUE_ETL_ROLE'
])

# Initialize Spark and Glue context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Initialize Glue Job
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Get the input S3 path
s3_path = args['S3_PATH']

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType

# Read Parquet file into DynamicFrame
dynamic_frame = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": [s3_path]},
    format="parquet"
)

# Define the columns to be selected
columns_to_read = [
    'PID', 'LEGAL_TYPE', 'ZONING_DISTRICT', 'ZONING_CLASSIFICATION', 'TO_CIVIC_NUMBER',
    'STREET_NAME', 'PROPERTY_POSTAL_CODE', 'CURRENT_LAND_VALUE',
    'CURRENT_IMPROVEMENT_VALUE', 'TAX_ASSESSMENT_YEAR', 'PREVIOUS_LAND_VALUE', 'PREVIOUS_IMPROVEMENT_VALUE',
    'YEAR_BUILT', 'BIG_IMPROVEMENT_YEAR', 'TAX_LEVY', 'NEIGHBOURHOOD_CODE', 'REPORT_YEAR'
]

# Convert to Spark DataFrame and select specific columns
df = dynamic_frame.toDF().select(columns_to_read)

# Data cleaning
df = df.dropna(how="any")  # Remove rows with any null values
df = df.dropDuplicates()  # Remove duplicate rows

# Format specific columns
df = df.withColumn("TAX_ASSESSMENT_YEAR", F.col("TAX_ASSESSMENT_YEAR").cast("int")) \
       .withColumn("REPORT_YEAR", F.col("REPORT_YEAR").cast("int"))
       
df = df \
    .withColumn("PID", F.col("PID").cast(StringType())) \
    .withColumn("LEGAL_TYPE", F.col("LEGAL_TYPE").cast(StringType())) \
    .withColumn("ZONING_DISTRICT", F.col("ZONING_DISTRICT").cast(StringType())) \
    .withColumn("ZONING_CLASSIFICATION", F.col("ZONING_CLASSIFICATION").cast(StringType())) \
    .withColumn("TO_CIVIC_NUMBER", F.col("TO_CIVIC_NUMBER").cast(StringType())) \
    .withColumn("STREET_NAME", F.col("STREET_NAME").cast(StringType())) \
    .withColumn("PROPERTY_POSTAL_CODE", F.col("PROPERTY_POSTAL_CODE").cast(StringType())) \
    .withColumn("CURRENT_LAND_VALUE", F.col("CURRENT_LAND_VALUE").cast(DecimalType(18, 2))) \
    .withColumn("CURRENT_IMPROVEMENT_VALUE", F.col("CURRENT_IMPROVEMENT_VALUE").cast(DecimalType(18, 2))) \
    .withColumn("TAX_ASSESSMENT_YEAR", F.col("TAX_ASSESSMENT_YEAR").cast(IntegerType())) \
    .withColumn("PREVIOUS_LAND_VALUE", F.col("PREVIOUS_LAND_VALUE").cast(DecimalType(18, 2))) \
    .withColumn("PREVIOUS_IMPROVEMENT_VALUE", F.col("PREVIOUS_IMPROVEMENT_VALUE").cast(DecimalType(18, 2))) \
    .withColumn("YEAR_BUILT", F.col("YEAR_BUILT").cast(IntegerType())) \
    .withColumn("BIG_IMPROVEMENT_YEAR", F.col("BIG_IMPROVEMENT_YEAR").cast(IntegerType())) \
    .withColumn("TAX_LEVY", F.col("TAX_LEVY").cast(DecimalType(18, 2))) \
    .withColumn("NEIGHBOURHOOD_CODE", F.col("NEIGHBOURHOOD_CODE").cast(StringType())) \
    .withColumn("REPORT_YEAR", F.col("REPORT_YEAR").cast(IntegerType()))
       
df.show(10)

# Convert cleaned DataFrame back to DynamicFrame
filtered_dynamic_frame = DynamicFrame.fromDF(df, glueContext, "filtered_dynamic_frame")

# Define the connection options for Redshift
conn_options = {
    "dbtable": args['REDSHIFT_TABLE_NAME'],
    "database": args['REDSHIFT_DATABASE_NAME'],
    "aws_iam_role": args["GLUE_ETL_ROLE"]
}

# Write the filtered data to Redshift
glueContext.write_dynamic_frame.from_jdbc_conf(
    frame = filtered_dynamic_frame, 
    catalog_connection = args['DC_CONNECTION_NAME'], 
    connection_options = conn_options, 
    redshift_tmp_dir = args['REDSHIFT_TEMP_DIR']
)

# End Glue Job
job.commit()