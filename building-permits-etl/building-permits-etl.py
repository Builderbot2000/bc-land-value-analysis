import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import col, from_json, lit
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, DoubleType

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

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, DateType

# Read Parquet file into DynamicFrame
dynamic_frame = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": [s3_path]},
    format="parquet"
)

# Define the column renaming map to convert camelCase to snake_case
column_rename_map = {
    'issueDate': 'issue_date',
    'projectValue': 'project_value',
    'issueYear': 'issue_year',
    'yearMonth': 'year_month',
    'geoLocalArea': 'geo_local_area',
    'typeOfWork': 'type_of_work',
    'permitCategory': 'permit_category',
    'propertyUse': 'property_use',
    'specificUseCategory': 'specific_use_category',
}

# Convert to dataframe
df = dynamic_frame.toDF()

# Define the columns to be selected (based on the new schema)
columns_to_read = [
    'issueDate', 'projectValue', 'typeOfWork', 'address',
    'permitCategory', 'propertyUse', 'specificUseCategory', 'issueYear', 'yearMonth',
    'geom', 'geoLocalArea'
]

# Select the required columns (ensure they're in the right order)
df = df.select(columns_to_read)

# Extract postal code from the address column using regex
# This regex assumes Canadian postal codes like 'V6E 3T1', adjust it as needed for your case
df = df.withColumn(
    "postal_code",
    F.regexp_extract(F.col("address"), r'([A-Za-z]\d[A-Za-z] \d[A-Za-z]\d)', 0)
)

# Data cleaning
df = df.dropna(how="any")  # Remove rows with any null values
df = df.dropDuplicates()  # Remove duplicate rows

# Format specific columns
df = df.withColumn("issueDate", F.col("issueDate").cast(DateType())) \
       .withColumn("projectValue", F.col("projectValue").cast(DecimalType(15, 2))) \
       .withColumn("issueYear", F.col("issueYear").cast(IntegerType())) \
       .withColumn("yearMonth", F.col("yearMonth").cast(StringType())) \
       .withColumn("geoLocalArea", F.col("geoLocalArea").cast(StringType())) \
       .withColumn("typeOfWork", F.col("typeOfWork").cast(StringType())) \
       .withColumn("address", F.col("address").cast(StringType())) \
       .withColumn("permitCategory", F.col("permitCategory").cast(StringType())) \
       .withColumn("propertyUse", F.col("propertyUse").cast(StringType())) \
       .withColumn("specificUseCategory", F.col("specificUseCategory").cast(StringType())) \
       .withColumn("geom", F.col("geom").cast(StringType()))

df.show(10)       

# Apply the renaming based on the map
for col in df.columns:
    if col in column_rename_map:
        df = df.withColumnRenamed(col, column_rename_map[col])

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
