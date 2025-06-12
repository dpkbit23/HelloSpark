from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, LongType

if __name__=="__main__":
    spark = SparkSession.builder.appName("LogValidation").master("local[3]").getOrCreate()


    schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("session", StructType([
        StructField("start", StringType(), True),
        StructField("end", StringType(), True)
    ]), True),
    StructField("device", StringType(), True)
    ])

    df_json = spark.read.option("mode", "PERMISSIVE") \
                .option("multiLine", True) \
                .option("columnNameOfCorruptRecord","_corrupt_record")\
               .schema(schema)\
               .json("data/user_session1.json")
    df_json.show()


    spark.stop()