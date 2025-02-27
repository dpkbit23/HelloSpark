from pyspark.sql import SparkSession
from pyspark.sql import functions
from pyspark.sql.functions import lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

if __name__ == "__main__":
    spark = SparkSession.builder.master("local[3]").appName("flexibleSchema").getOrCreate()

    schema = StructType([
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True)
    ])

    df = spark.read.schema(schema).json("data/people.json")
    df.printSchema()
    df.show()
    df_new_col = df.withColumn("city", lit(None).cast(StringType()))
    df_new_col = spark.read.json("data/people.json")
    df_new_col.printSchema()
    df_new_col.show()
    df_new_col.write.mode("overwrite").json("Output/flexibleSchema.json")
    spark.stop()
