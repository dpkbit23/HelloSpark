from pyspark.sql import SparkSession

# Start Spark
spark = SparkSession.builder \
    .appName("JobStageTaskExample") \
    .getOrCreate()

# Create DataFrames
df1 = spark.range(2, 200, 1).toDF("id1")     # 2 ... 199
df2 = spark.range(4, 200, 2).toDF("id2")     # 4 ... 198

# Increase partitions for visible parallelism
df1 = df1.repartition(6)
df2 = df2.repartition(4)

print("DF1:")
df1.show(5)
print("DF2:")
df2.show(5)

# Perform a join (shuffle operation)
df3 = df1.join(df2, df1.id1 == df2.id2, "inner")

# Add transformation to see execution plan
df4 = df3.withColumn("sum_col", df3.id1 + df3.id2)

# Trigger an action
result = df4.count()

print(f"Final Count = {result}")

input("Press ENTER to keep Spark UI open and inspect...")
