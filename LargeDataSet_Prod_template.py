import sys
import time
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import col, broadcast
from pyspark.sql.utils import AnalysisException

# 1. Define configuration parameters
class Config:
    app_name = "ProdBigDataProcessing"
    input_path = "/path/to/input/parquet_or_delta"
    output_path = "/path/to/output/delta_table"
    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("date", StringType(), True),
    ])
    output_format = "delta"  # or "parquet"
    shuffle_partitions = 1000
    enable_adaptive_execution = True
    retry_attempts = 3
    retry_delay_secs = 10

# 2. Set up Spark session
def create_spark_session(config):
    builder = SparkSession.builder.appName(config.app_name)
    builder = builder.config("spark.sql.shuffle.partitions", config.shuffle_partitions)
    if config.enable_adaptive_execution:
        builder = builder.config("spark.sql.adaptive.enabled", "true") \
                         .config("spark.sql.adaptive.skewJoin.enabled", "true")
    return builder.getOrCreate()

# 3. Main processing logic
def process_data(spark, config):
    try:
        print("[INFO] Reading data...")
        df = spark.read.schema(config.schema).parquet(config.input_path)

        print("[INFO] Preprocessing data...")
        df = df.filter((col("amount") > 0) & (col("date").isNotNull())) \
               .select("id", "amount", "date")

        # Dynamic partitioning
        print("[INFO] Calculating dynamic partitions...")
        num_rows = df.count()
        target_partition_size_mb = 200
        estimated_row_size_bytes = 500
        target_rows_per_partition = (target_partition_size_mb * 1024 * 1024) // estimated_row_size_bytes
        num_partitions = max(1, num_rows // target_rows_per_partition)

        df = df.repartition(int(num_partitions), "date")
        df.cache()
        df.count()  # materialize cache

        # Write Output
        print("[INFO] Writing processed data...")
        df.write.format(config.output_format) \
            .mode("overwrite") \
            .partitionBy("date") \
            .save(config.output_path)

        print("[SUCCESS] Data processing completed successfully.")

    except AnalysisException as ae:
        print(f"[ERROR] AnalysisException occurred: {ae}")
        raise
    except Exception as e:
        print(f"[ERROR] General Exception occurred: {e}")
        raise

# 4. Retry logic
def main():
    config = Config()
    attempt = 0
    while attempt < config.retry_attempts:
        try:
            spark = create_spark_session(config)
            process_data(spark, config)
            break  # success, exit retry loop
        except Exception as e:
            attempt += 1
            if attempt == config.retry_attempts:
                print(f"[FAILURE] Job failed after {attempt} attempts. Exiting.")
                sys.exit(1)
            else:
                print(f"[WARN] Retry {attempt}/{config.retry_attempts} after error: {e}")
                time.sleep(config.retry_delay_secs)
        finally:
            try:
                spark.stop()
            except Exception as e:
                print(f"[WARN] Error while stopping Spark session: {e}")

if __name__ == "__main__":
    main()
