import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, regexp_extract
from pyspark.sql.types import StringType, IntegerType, DecimalType, DateType

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

# Read CSV file into DynamicFrame
dynamic_frame = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": [s3_path]},
    format="csv",
    format_options={
        "withHeader": True,
        "separator": ";",  # Updated separator to handle semicolons
        "quoteChar": "\"",
        "escaper": "\\"
    }
)

# Convert to dataframe
df = dynamic_frame.toDF()

# Updated column mapping to snake_case (removing non-relevant columns for Redshift table)
column_rename_map = {
    'PermitNumber': 'permit_number',
    'PermitNumberCreatedDate': 'permit_number_created_date',
    'IssueDate': 'issue_date',
    'PermitElapsedDays': 'permit_elapsed_days',
    'ProjectValue': 'project_value',
    'TypeOfWork': 'type_of_work',
    'Address': 'address',
    'ProjectDescription': 'project_description',
    'PermitCategory': 'permit_category',
    'Applicant': 'applicant',
    'ApplicantAddress': 'applicant_address',
    'PropertyUse': 'property_use',
    'SpecificUseCategory': 'specific_use_category',
    'BuildingContractor': 'building_contractor',
    'BuildingContractorAddress': 'building_contractor_address',
    'IssueYear': 'issue_year',
    'GeoLocalArea': 'geo_local_area',
    'Geom': 'geom',
    'YearMonth': 'year_month',
    'geo_point_2d': 'geo_point_2d'
}

# Define the columns to be selected (updated based on the new schema)
columns_to_read = [
    'PermitNumber', 'IssueDate', 'ProjectValue', 'TypeOfWork', 'Address', 
    'PermitCategory', 'PropertyUse', 'SpecificUseCategory', 'IssueYear', 
    'GeoLocalArea', 'Geom', 'YearMonth', 'geo_point_2d'
]

# Select the required columns
df = df.select([col(column) for column in columns_to_read])

# Rename columns to snake_case
for old_col, new_col in column_rename_map.items():
    df = df.withColumnRenamed(old_col, new_col)

# Extract postal code from the address column using regex
df = df.withColumn(
    "postal_code",
    regexp_extract(col("address"), r'([A-Za-z]\d[A-Za-z] \d[A-Za-z]\d)', 0)
)

# Data cleaning
df = df.dropna(how="any")  # Remove rows with any null values
df = df.dropDuplicates()  # Remove duplicate rows

# Format specific columns based on Redshift table schema
df = df.withColumn("issue_date", col("issue_date").cast(DateType())) \
       .withColumn("project_value", col("project_value").cast(DecimalType(15, 2))) \
       .withColumn("issue_year", col("issue_year").cast(IntegerType())) \
       .withColumn("year_month", col("year_month").cast(StringType())) \
       .withColumn("geo_local_area", col("geo_local_area").cast(StringType())) \
       .withColumn("type_of_work", col("type_of_work").cast(StringType())) \
       .withColumn("address", col("address").cast(StringType())) \
       .withColumn("permit_category", col("permit_category").cast(StringType())) \
       .withColumn("property_use", col("property_use").cast(StringType())) \
       .withColumn("specific_use_category", col("specific_use_category").cast(StringType())) \
       .withColumn("geom", col("geom").cast(StringType())) \
       .withColumn("postal_code", col("postal_code").cast(StringType()))  # postal_code column to match Redshift schema

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
    frame=filtered_dynamic_frame, 
    catalog_connection=args['DC_CONNECTION_NAME'], 
    connection_options=conn_options, 
    redshift_tmp_dir=args['REDSHIFT_TEMP_DIR']
)

# End Glue Job
job.commit()

