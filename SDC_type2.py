from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
import hashlib
from datetime import datetime

if __name__ == "__main__":
   spark = SparkSession.builder.master("local[*]").appName("SCD_Type2").getOrCreate()

   schema_current = StructType([
       StructField("customer_id", IntegerType(), False),
       StructField("name", StringType(), True),
       StructField("city", StringType(), True),
       StructField("start_date", StringType(), True),
       StructField("end_date", StringType(), True),
       StructField("is_current", BooleanType(), True),
       StructField("hash", StringType(), True),
   ])

   data_current = [
       (1, "Alice", "NY", "2023-01-01", None, True, "a555d97ceca61b04469209f1195c7dfb"),  # Same hash
       (2, "Bob", "LA", "2023-01-05", None, True, "67c6fede1d8b423dddfb82c7a1e8a5bc"),  # Will change
       (3, "Charlie", "SF", "2023-01-10", None, True, "b1ee77ebf81f4d95f44c8e293c3db779"),  # Will be untouched
   ]

   df_current = spark.createDataFrame(data_current, schema=schema_current)

   # Incoming data
   schema_incoming = StructType([
       StructField("customer_id", IntegerType(), False),
       StructField("name", StringType(), True),
       StructField("city", StringType(), True),
   ])

   data_incoming = [
       (1, "Alice", "NY"),  # No change
       (2, "Bob", "Chicago"),  # Changed
       (4, "David", "Miami"),  # New
   ]

   df_incoming = spark.createDataFrame(data_incoming, schema=schema_incoming)

   # Step 1: Create hash UDF for change detection
   hash_udf = F.udf(lambda n, c: hashlib.md5(f"{n}|{c}".encode()).hexdigest(), StringType())

   df_incoming = df_incoming.withColumn("hash", hash_udf("name", "city"))

   # Join current (active) records with incoming data
   df_join = df_incoming.alias("new").join(
       df_current.filter("is_current = true").alias("curr"),
       on="customer_id",
       how="left"
   )

   unchanged = df_join.filter(F.col("new.hash") == F.col("curr.hash"))
   changed = df_join.filter((F.col("new.hash") != F.col("curr.hash")) | F.col("curr.hash").isNull())

   today = datetime.today().strftime('%Y-%m-%d')

   expired = changed.filter(F.col("curr.hash").isNotNull()).select(
       "customer_id",
       F.col("curr.name").alias("name"),
       F.col("curr.city").alias("city"),
       F.col("curr.start_date").alias("start_date"),
       F.lit(today).alias("end_date"),
       F.lit(False).alias("is_current"),
       F.col("curr.hash").alias("hash")
   )
   new_records = changed.select(
       "customer_id",
       F.col("new.name").alias("name"),
       F.col("new.city").alias("city"),
       F.lit(today).alias("start_date"),
       F.lit(None).cast(StringType()).alias("end_date"),
       F.lit(True).alias("is_current"),
       F.col("new.hash").alias("hash")
   )

   unchanged_final = unchanged.select(
       "customer_id",
       F.col("curr.name").alias("name"),
       F.col("curr.city").alias("city"),
       F.col("curr.start_date").alias("start_date"),
       F.col("curr.end_date").alias("end_date"),
       F.col("curr.is_current").alias("is_current"),
       F.col("curr.hash").alias("hash")
   )
   df_scd2 = unchanged_final.unionByName(expired).unionByName(new_records).orderBy("customer_id", "start_date")
   df_scd2.show(truncate=False)
'''
+-----------+-----+-------+----------+----------+----------+--------------------------------+
|customer_id|name |city   |start_date|end_date  |is_current|hash                            |
+-----------+-----+-------+----------+----------+----------+--------------------------------+
|1          |Alice|NY     |2023-01-01|NULL      |true      |a555d97ceca61b04469209f1195c7dfb|
|2          |Bob  |LA     |2023-01-05|2025-05-24|false     |67c6fede1d8b423dddfb82c7a1e8a5bc|
|2          |Bob  |Chicago|2025-05-24|NULL      |true      |ceab4546477033f1a4e87954246aa9d9|
|4          |David|Miami  |2025-05-24|NULL      |true      |c3c446a08df1696415711fe893ca3227|
+-----------+-----+-------+----------+----------+----------+--------------------------------+

'''