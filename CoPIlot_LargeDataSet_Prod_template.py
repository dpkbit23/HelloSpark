import sys
import time
import logging
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import col
from pyspark.sql.utils import AnalysisException

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# Define configuration parameters
class Config:
    app_name = "ProdBigDataProcessing"
    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("date", StringType(), True),
    ])
    shuffle_partitions = 1000
    enable_adaptive_execution = True
    retry_attempts = 3
    retry_delay_secs = 10
    target_partition_size_mb = 200
    estimated_row_size_bytes = 500


def parse_arguments():
    parser = argparse.ArgumentParser(description="Spark Data Processing")
    parser.add_argument("--input-path", required=True, help="Path to input data (parquet/delta)")
    parser.add_argument("--output-path", required=True, help="Path to output delta table")
    parser.add_argument("--output-format", default="delta", help="Output format (delta/parquet)")
    return parser.parse_args()

def create_spark_session(config: Config) -> SparkSession:
    builder = SparkSession.builder.appName(config.app_name)
    builder = builder.config("spark.sql.shuffle.partitions", config.shuffle_partitions)
    if config.enable_adaptive_execution:
        builder = builder.config("spark.sql.adaptive.enabled", "true") \
                         .config("spark.sql.adaptive.skewJoin.enabled", "true")
    return builder.getOrCreate()

def read_data(spark: SparkSession, input_path: str, schema: StructType):
    logger.info("Reading data...")
    return spark.read.schema(schema).parquet(input_path)

def preprocess_data(df):
    logger.info("Preprocessing data...")
    return df.filter((col("amount") > 0) & (col("date").isNotNull())) \
             .select("id", "amount", "date")

def calculate_partitions(df, config: Config):
    logger.info("Calculating dynamic partitions...")
    num_rows = df.count()
    target_rows_per_partition = (config.target_partition_size_mb * 1024 * 1024) // config.estimated_row_size_bytes
    return max(1, num_rows // target_rows_per_partition)

def process_data(spark: SparkSession, config: Config, input_path: str, output_path: str, output_format: str):
    try:
        df = read_data(spark, input_path, config.schema)
        df = preprocess_data(df)
        num_partitions = calculate_partitions(df, config)
        df = df.repartition(num_partitions, "date")
        df.cache()
        df.count()  # materialize cache

        logger.info("Writing processed data...")
        df.write.format(output_format).mode("overwrite").partitionBy("date").save(output_path)

        logger.info("Data processing completed successfully.")
    except AnalysisException as ae:
        logger.error(f"AnalysisException occurred: {ae}")
        raise
    except Exception as e:
        logger.error(f"General Exception occurred: {e}")
        raise

def main():
    args = parse_arguments()
    config = Config()
    attempt = 0
    while attempt < config.retry_attempts:
        try:
            spark = create_spark_session(config)
            process_data(spark, config, args.input_path, args.output_path, args.output_format)
            break
        except Exception as e:
            attempt += 1
            if attempt == config.retry_attempts:
                logger.error(f"Job failed after {attempt} attempts. Exiting.")
                sys.exit(1)
            else:
                logger.warning(f"Retry {attempt}/{config.retry_attempts} after error: {e}")
                time.sleep(config.retry_delay_secs)
        finally:
            if 'spark' in locals():
                try:
                    spark.stop()
                except Exception as e:
                    logger.warning(f"Error while stopping Spark session: {e}")

if __name__ == "__main__":
    main()