import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# Initialize Glue context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

# Read from Bronze layer
bronze_df = glueContext.create_dynamic_frame.from_catalog(
    database="thera_bronze",
    table_name="bronze_crunchbase_ext"
)

# Transform data
def transform_record(record):
    # Clean and transform the data
    record["company_name"] = record.get("Organization Name", "")
    record["website_url"] = record.get("Website", "")
    record["linkedin_url"] = record.get("LinkedIn", "")
    record["headquarters_location"] = record.get("Headquarters Location", "")
    record["industries"] = record.get("Industries", "")
    record["description"] = record.get("Description", "")
    record["founded_date"] = record.get("Founded Date", "")
    record["employee_count"] = record.get("Number of Employees", "")
    record["total_funding_usd"] = record.get("Total Funding Amount (in USD)", "")
    record["last_funding_date"] = record.get("Last Funding Date", "")
    record["funding_status"] = record.get("Funding Status", "")
    record["country"] = record.get("country", "")
    return record

# Apply transformation
transformed_df = Map.apply(frame=bronze_df, f=transform_record)

# Write to Silver layer
glueContext.write_dynamic_frame.from_options(
    frame=transformed_df,
    connection_type="s3",
    connection_options={
        "path": "s3://thera-curated-805595753342/silver/companies/"
    },
    format="parquet"
)

print("ETL job completed successfully!")
job.commit()
