from pyspark.sql import SparkSession


spark = SparkSession.builder \
                    .appName("CPT Codes Ingestion") \
                    .getOrCreate()

# configure variables
BUCKET_NAME = "healthcare-bucket-28-07-2025"
CPT_BUCKET_PATH = f"gs://{BUCKET_NAME}/landing/cptcodes/*.csv"
BQ_TABLE = "active-district-466711-i0.bronze_dataset.cpt_codes"
TEMP_GCS_BUCKET = f"{BUCKET_NAME}/temp/"

# read from cpt
cptcodes_df = spark.read.csv(CPT_BUCKET_PATH, header=True)

# replace spaces with underscore
for col in cptcodes_df.columns:
    new_col = col.replace(" ", "_").lower()
    cptcodes_df = cptcodes_df.withColumnRenamed(col, new_col)

# write to bigquery
(cptcodes_df.write
            .format("bigquery")
            .option("table", BQ_TABLE)
            .option("temporaryGcsBucket", TEMP_GCS_BUCKET)
            .mode("overwrite")
            .save())