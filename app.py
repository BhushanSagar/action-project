from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, when, lit, collect_list, concat_ws

# Initialize Spark session
spark = SparkSession.builder.appName("DataCleaning").getOrCreate()

# Define schema for CSV
data_schema = "ID INT, Name STRING, Email STRING, Phone STRING, Address STRING"

# Load CSV file
df = spark.read.option("header", "true").schema(data_schema).csv("data.csv")

# Email & Phone validation regex
email_regex = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
phone_regex = r'^[0-9\-\s]+$'

# Identify issues in data
df_with_issues = df.withColumn(
    "Issues",
    when(col("Name").isNull(), lit("Missing Name"))
    .when(~col("Email").rlike(email_regex), lit("Invalid or Missing Email"))
    .when(~col("Phone").rlike(phone_regex), lit("Invalid or Missing Phone"))
    .when((col("Address").isNotNull()) & (trim(col("Address")) != col("Address")), lit("Address has leading/trailing spaces"))
    .when(col("Address").isNull(), lit("Missing Address"))
)

# Collect issues into a single list
log_data = df_with_issues.filter(col("Issues").isNotNull()) \
    .select(concat_ws(": ", col("ID"), col("Issues")).alias("LogEntry")) \
    .agg(collect_list("LogEntry")).collect()

# Write log data to a text file
log_file = "log_issues.txt"
with open(log_file, "w") as log:
    for row in log_data:
        for entry in row[0]:  # Since collect_list returns a list
            log.write(entry + "\n")

# Clean Data
clean_df = df.withColumn("Email", trim(col("Email"))) \
             .withColumn("Phone", trim(col("Phone"))) \
             .withColumn("Address", trim(col("Address")))

# Save cleaned data
clean_df.coalesce(1).write.mode("overwrite").option("header", "true").csv("cleaned_data.csv")

print("Validation completed. Check log_issues.txt for details.")
