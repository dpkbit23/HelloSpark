from logging import Logger
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
from lib.logger import Log4j


# Constants
DEFAULT_PARTITIONS = 3
NUM_ROWS = 1000000

def main():
    # Initialize Spark session
    spark = SparkSession.builder \
                        .appName("Salting") \
                        .master("local[*]") \
                        .getOrCreate()

    logger = Log4j(spark)

    # Set Spark configurations
    spark.conf.set("spark.sql.shuffle.partitions", str(DEFAULT_PARTITIONS))
    spark.conf.set("spark.sql.adaptive.enabled", "false")

    logger.info("Spark configurations set.")

    # Create uniform DataFrame
    df_uniform = spark.createDataFrame([i for i in range(NUM_ROWS)], IntegerType())
    log_partitions(df_uniform, "Uniform DataFrame", logger)

    # Create skewed DataFrame
    df_skew = create_skewed_df(spark)
    log_partitions(df_skew, "Skewed DataFrame", logger)

    # Join DataFrames
    df_joined = df_skew.join(df_uniform, 'value', 'inner')
    log_partitions(df_joined, "Joined DataFrame", logger)

    # Apply salting
    apply_salting(spark, df_skew, logger)

    input("Enter any key to continue")

def log_partitions(df, description, logger):
    df.withColumn("partition", F.spark_partition_id()) \
      .groupBy("partition") \
      .count() \
      .orderBy("partition") \
      .show(15, False)
    logger.info(f"{description} partitioned and counted")

def create_skewed_df(spark):
    df0 = spark.createDataFrame([0] * NUM_ROWS, IntegerType()).repartition(1)
    df1 = spark.createDataFrame([1] * 20, IntegerType()).repartition(1)
    df2 = spark.createDataFrame([2] * 15, IntegerType()).repartition(1)
    df3 = spark.createDataFrame([3] * 10, IntegerType()).repartition(1)
    return df0.union(df1).union(df2).union(df3)

def apply_salting(spark, df_skew, logger):
    salt_number = int(spark.conf.get("spark.sql.shuffle.partitions"))
    logger.info(f"Salt number: {salt_number}")

    df_skew.withColumn("salt", (F.rand() * salt_number).cast("int")) \
           .groupBy("value", "salt") \
           .agg(F.count("value").alias("count")) \
           .groupBy("value") \
           .agg(F.sum("count").alias("count")) \
           .show(15, False)

if __name__ == "__main__":
    main()