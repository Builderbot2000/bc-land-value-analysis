import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
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

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Use the S3 path provided as a parameter
s3_path = args['S3_PATH']

# Define the schema with the updated naming convention
schema = StructType([
    StructField("FOLDER_YEAR", IntegerType(), nullable=False),  # INT, NOT NULL
    StructField("STATUS", StringType(), nullable=True),  # VARCHAR(255)
    StructField("ISSUED_DATE", StringType(), nullable=True),  # VARCHAR(255)
    StructField("BUSINESS_NAME", StringType(), nullable=False),  # VARCHAR(255)
    StructField("BUSINESS_TRADE_NAME", StringType(), nullable=True),  # VARCHAR(255)
    StructField("BUSINESS_TYPE", StringType(), nullable=True),  # VARCHAR(255)
    StructField("BUSINESS_SUBTYPE", StringType(), nullable=True),  # VARCHAR(255)
    StructField("POSTAL_CODE", StringType(), nullable=False), # VARCHAR(255)
    StructField("STREET_NAME", StringType(), nullable=True),  # VARCHAR(255)
    StructField("LOCAL_AREA", StringType(), nullable=True),  # VARCHAR(255)
    StructField("NUMBER_OF_EMPLOYEES", IntegerType(), nullable=True),  # INT
])

# Read the Parquet file into a DynamicFrame
dynamic_frame = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": [s3_path]},
    format="parquet"
)

# Convert the DynamicFrame to a Spark DataFrame
data_frame = dynamic_frame.toDF()

# Rename columns to the new naming convention
data_frame = data_frame.select(
    F.col("folderyear").alias("FOLDER_YEAR"),
    F.col("status").alias("STATUS"),
    F.col("issueddate").alias("ISSUED_DATE"),
    F.col("businessname").alias("BUSINESS_NAME"),
    F.col("businesstradename").alias("BUSINESS_TRADE_NAME"),
    F.col("businesstype").alias("BUSINESS_TYPE"),
    F.col("businesssubtype").alias("BUSINESS_SUBTYPE"),
    F.col("postalcode").alias("POSTAL_CODE"),
    F.col("street").alias("STREET_NAME"),
    F.col("localarea").alias("LOCAL_AREA"),
    F.col("numberofemployees").alias("NUMBER_OF_EMPLOYEES")
)

# Filter out rows where required columns are null
data_frame = data_frame.filter(F.col("FOLDER_YEAR").isNotNull())
data_frame = data_frame.filter(F.col("POSTAL_CODE").isNotNull())
data_frame = data_frame.filter(F.col("BUSINESS_NAME").isNotNull())

# Cast fields to their appropriate types
data_frame = data_frame.withColumn("FOLDER_YEAR", F.col("FOLDER_YEAR").cast(IntegerType()))
data_frame = data_frame.withColumn("NUMBER_OF_EMPLOYEES", F.col("NUMBER_OF_EMPLOYEES").cast(IntegerType()))

# Set the maximum length for string fields (adjust based on your Redshift column limits)
MAX_STRING_LENGTH = 255

# Truncate string columns to fit the length limit
for column in ["STATUS", "ISSUED_DATE", "BUSINESS_NAME", "BUSINESS_TRADE_NAME", "BUSINESS_TYPE", "BUSINESS_SUBTYPE", "POSTAL_CODE", "STREET_NAME", "LOCAL_AREA"]:
    data_frame = data_frame.withColumn(
        column,
        F.when(F.length(F.col(column)) > MAX_STRING_LENGTH, F.substring(F.col(column), 1, MAX_STRING_LENGTH))
         .otherwise(F.col(column))
    )

# Apply the schema to ensure consistency
filtered_data_frame = spark.createDataFrame(data_frame.rdd, schema)

# Convert the filtered DataFrame back to a DynamicFrame
filtered_dynamic_frame = DynamicFrame.fromDF(filtered_data_frame, glueContext, "filtered_dynamic_frame")

# Define the connection options for Redshift
conn_options = {
    "dbtable": args['REDSHIFT_TABLE_NAME'],
    "database": args['REDSHIFT_DATABASE_NAME'],
    "aws_iam_role": args["GLUE_ETL_ROLE"]
}

# Write the filtered data to Redshift
glueContext.write_dynamic_frame.from_jdbc_conf(
    frame=filtered_dynamic_frame, 
    catalog_connection=args['DC_CONNECTION_NAME'], 
    connection_options=conn_options, 
    redshift_tmp_dir=args['REDSHIFT_TEMP_DIR']
)

# Commit the job
job.commit()