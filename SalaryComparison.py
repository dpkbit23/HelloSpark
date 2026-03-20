from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import avg, col, round, dense_rank

if __name__ == "__main__":
    spark = SparkSession.builder.master("local[*]").appName("salaryComparison").getOrCreate()

    data = [
        (1, "A", "HR", 50000),
        (2, "B", "HR", 60000),
        (3, "C", "IT", 70000),
        (4, "D", "IT", 80000),
        (5, "E", "Finance", 75000),
        (6, "F", "Finance", 85000),
        (7,"G", "Sales", 100000)
    ]
    columns = ["emp_id","name","Dept","salary"]
    df = spark.createDataFrame(data,columns)
    df_avg_sal = df.groupBy("dept").agg(avg("salary").alias("avgSal"))

    df_sal_join = df.join(df_avg_sal, on="dept", how="inner")
    df.join(df_avg_sal, on="dept", how="inner").explain("extended")
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
    df_sal_comparison = (df_sal_join.withColumn("SalDiff", col("avgSal") - col("salary"))
                         .withColumn("Diff%", round((col("SalDiff")/ col("salary"))*100, 2)))
    df.show()
    df_avg_sal.show()
    df_sal_comparison.show()

    window_spec = Window.partitionBy("Dept").orderBy(col("salary").desc())
    df_high_sal = df.withColumn("rank", dense_rank().over(window_spec))
    df_sec_high_sal = df_high_sal.filter(col("rank")==2)
    df_sec_high_sal.show()

    '''
    
   # Get existing records that are not in the incoming dataset
   not_in_incoming = df_current.filter("is_current = true").join(
       df_incoming.select("customer_id"),
       on="customer_id",
       how="left_anti"
   )
   

        +-------+------+----+------+--------+-------+-----+
        |   Dept|emp_id|name|salary|  avgSal|SalDiff|Diff%|
        +-------+------+----+------+--------+-------+-----+
        |     HR|     1|   A| 50000| 55000.0| 5000.0| 10.0|
        |     HR|     2|   B| 60000| 55000.0|-5000.0|-8.33|
        |     IT|     3|   C| 70000| 75000.0| 5000.0| 7.14|
        |     IT|     4|   D| 80000| 75000.0|-5000.0|-6.25|
        |Finance|     5|   E| 75000| 80000.0| 5000.0| 6.67|
        |Finance|     6|   F| 85000| 80000.0|-5000.0|-5.88|
        |  Sales|     7|   G|100000|100000.0|    0.0|  0.0|
        +-------+------+----+------+--------+-------+-----+
    '''