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
