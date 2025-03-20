from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col, arrays_zip, expr, explode

if __name__=="__main__":
    spark = SparkSession.builder.appName("pipeDelimiterBreak").master("local[3]").getOrCreate()

    data = spark.read.text("data/singleRowPipe.txt")
    raw_df = data.toDF("rawData")

    split_df = raw_df.withColumn("splitData", split(col("rawData"),"\\|"))

    group_df = (((split_df.withColumn("name", expr("slice(splitData,1, size(splitData))"))
                    .withColumn("dept", expr("slice(splitData,2, size(splitData))")))
                    .withColumn("exp", expr("slice(splitData,3, size(splitData))")))
                    .withColumn("tech", expr("slice(splitData,4, size(splitData))")))

    zipped_df = group_df.withColumn("grouped", arrays_zip("name","dept","exp","tech"))

    df_exploded = zipped_df.select(explode(col("grouped")).alias("row"))

    df_final = df_exploded.select(
        col("row.name").alias("Name"),
        col("row.dept").alias("Dept"),
        col("row.exp").alias("Exp"),
        col("row.tech").alias("Tech")
    )
    df_final.show(truncate=False)