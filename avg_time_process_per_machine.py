from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, col, max as Fmax, min as Fmin, avg, round

if __name__ == "__main__":
    spark = SparkSession.builder.appName("avg_time").getOrCreate()

    data_df = [(0,1,'start','18.891'),
               (1,0,'end',81.874),
               (0,0,'start',37.019),
               (0,1,'end',38.098),
               (1,0,'start',25.135),
               (1,1,'start',23.355),
               (0,0,'end',40.222),
               (1,1,'end',90.302)
            ]
    schema = ["machine_id","process_id","activity_type","timestamp"]

    activity_df = spark.createDataFrame(data_df,schema)
    activity_df = (activity_df.withColumn("end_time", expr("case when activity_type = 'end' then timestamp end").cast("double"))
                   .withColumn("start_time", expr("case when activity_type = 'start' then timestamp end").cast("double"))
                   )
    df_pair = activity_df.groupBy(col("machine_id"),col("process_id")).agg(Fmax("start_time").alias("start_time"), Fmin("end_time").alias("end_time"))
    df_time = df_pair.withColumn("processing_time", col("end_time") - col("start_time"))
    df_result = df_time.groupBy("machine_id").agg(round(avg("processing_time"),3).alias("processing_time"))
    df_result.show()