from pyspark.sql import SparkSession


from pyspark.sql.functions import input_file_name, when, lit

# Create Spark session
spark = SparkSession.builder \
                    .appName("Healthcare Claims Ingestion") \
                    .getOrCreate()
                    
                    
BUCKET_NAME = "healthcare-bucket-28-07-2025"       
CLAIMS_BUCKET_PATH = f"gs://{BUCKET_NAME}/landing/claims/*.csv"
BQ_TABLE = "active-district-466711-i0.bronze_dataset.claims"
TEMP_GCS_BUCKET = f"{BUCKET_NAME}/temp/"
                    
                    
claims_df = spark.read.csv(CLAIMS_BUCKET_PATH, header=True)

claims_df = (claims_df
                .withColumn("datasource", 
                              when(input_file_name().contains("hospital2"), "hosb")
                             .when(input_file_name().contains("hospital1"), "hosa").otherwise("None")))
# dropping dupplicates if any
claims_df = claims_df.dropDuplicates()


# write to bigquery
(claims_df.write
            .format("bigquery")
            .option("table", BQ_TABLE)
            .option("temporaryGcsBucket", TEMP_GCS_BUCKET)
            .mode("overwrite")
            .save())
