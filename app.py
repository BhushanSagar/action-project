from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_extract, trim, when
import re

# Initialize Spark session
spark = SparkSession.builder.appName("DataCleaning").getOrCreate()

# Define schema for CSV (optional, can be inferred)
data_schema = "ID INT, Name STRING, Email STRING, Phone STRING, Address STRING"

# Load CSV file
df = spark.read.option("header", "true").schema(data_schema).csv("data.csv")

# Function to validate email (keep your original regex)
def is_valid_email(email):
    return bool(re.match(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$', email))

# Function to validate phone number
def is_valid_phone(phone):
    return bool(re.match(r'^[0-9\-\s]+$', phone))

# Open log file in append mode
log_file = "log.txt"
with open(log_file, "a") as log:
    # Process data and log issues
    for row in df.collect():
        issues = []
        
        if not row.Name:
            issues.append("Missing Name")
        if not row.Email or not is_valid_email(row.Email):
            issues.append("Invalid or Missing Email")
        if not row.Phone or not is_valid_phone(row.Phone):
            issues.append("Invalid or Missing Phone")
        if row.Address and row.Address.strip() != row.Address:
            issues.append("Address has leading/trailing spaces")
        if not row.Address:
            issues.append("Missing Address")
        
        if issues:
            log.write(f"Row {row.ID}: {', '.join(issues)}\n")

# Clean Data (using Spark's built-in functions)
clean_df = df.withColumn("Email", trim(col("Email"))) \
             .withColumn("Phone", trim(col("Phone"))) \
             .withColumn("Address", trim(col("Address")))

# Save cleaned data (using coalesce to output to a single file)
clean_df.coalesce(1).overwrite.option("header", "true").csv("cleaned_data.csv")

print("Validation completed. Check log.txt for details.")
