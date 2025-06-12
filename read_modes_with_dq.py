
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from pyspark.sql.functions import col, lit

spark = SparkSession.builder.appName("ParquetReadModesWithDQ").getOrCreate()

# Step 1: Load CSV and write to Parquet (simulate daily batch)
csv_path = "data/transactions.csv"
parquet_path = "data/transactions.parquet"

df_csv = spark.read.option("header", True).csv(csv_path)
df_csv.write.mode("overwrite").parquet(parquet_path)

# Step 2: Define strict schema
schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("customer", StringType(), True),
    StructField("amount", DoubleType(), True)
])

def read_with_mode(mode_name):
    try:
        df = spark.read.option("mode", mode_name) \
                       .option("columnNameOfCorruptRecord", "_corrupt_record") \
                       .schema(schema) \
                       .parquet(parquet_path)
        status = "success"
        return df.withColumn("read_mode", lit(mode_name)), status
    except Exception as e:
        print(f"[{{mode_name}}] ERROR: {{e}}")
        return None, "fail"

df_permissive, status_p = read_with_mode("PERMISSIVE")
df_dropmalformed, status_d = read_with_mode("DROPMALFORMED")
df_failfast, status_f = read_with_mode("FAILFAST")

from pyspark.sql.functions import count

def generate_dq_report(df, mode):
    if df is None:
        return spark.createDataFrame([(mode, "load_failed", 0)], ["mode", "status", "count"])

    corrupt_col_exists = "_corrupt_record" in df.columns

    valid_rows = df.filter(col("_corrupt_record").isNull()) if corrupt_col_exists else df
    corrupt_rows = df.filter(col("_corrupt_record").isNotNull()) if corrupt_col_exists else spark.createDataFrame([], df.schema)

    total = df.count()
    valid = valid_rows.count()
    corrupt = corrupt_rows.count()

    return spark.createDataFrame([
        (mode, "valid", valid),
        (mode, "corrupt", corrupt),
        (mode, "total", total)
    ], ["mode", "status", "count"])

dq_reports = []

if status_p == "success":
    dq_reports.append(generate_dq_report(df_permissive, "PERMISSIVE"))

if status_d == "success":
    dq_reports.append(generate_dq_report(df_dropmalformed, "DROPMALFORMED"))

if status_f == "success":
    dq_reports.append(generate_dq_report(df_failfast, "FAILFAST"))
else:
    dq_reports.append(spark.createDataFrame([("FAILFAST", "load_failed", 0)], ["mode", "status", "count"]))

dashboard_df = dq_reports[0]
for r in dq_reports[1:]:
    dashboard_df = dashboard_df.union(r)

dashboard_df.orderBy("mode", "status").show()
