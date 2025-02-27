from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, round

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
    df_sal_comparison = (df_sal_join.withColumn("SalDiff", col("avgSal") - col("salary"))
                         .withColumn("Diff%", round((col("SalDiff")/ col("salary"))*100, 2)))
    df.show()
    df_avg_sal.show()
    df_sal_comparison.show()

    '''
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