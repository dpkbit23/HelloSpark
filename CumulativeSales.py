from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import *
from pyspark.sql.types import *



if __name__ == "__main__":
    spark = SparkSession.builder.appName("CumulativeSales").master("local[3]").getOrCreate()

    data = [("East", "January", 200), ("East", "Feb", 300),
            ("East", "Mar", 250), ("West", "Jan", 400),
            ("West", "Feb", 350), ("West", "Mar", 450),
            ("North", "Jan", 300), ("North", "Feb", 300),
            ("North", "Mar", 270), ("South", "Jan", 500),
            ("South", "Feb", 750), ("South", "Mar", 650),("South", "April", 650)
            ]

    columns = ["Region", "Month", "Sales"]

    df = spark.createDataFrame(data, columns)
    month_map = {
        "Jan": 1, "January": 1,
        "Feb": 2, "Mar": 3, "Apr": 4, "May": 5, "Jun": 6, "Jul": 7,
        "Aug": 8, "Sep": 9, "Oct": 10, "Nov": 11, "Dec": 12,
    }

    @udf(IntegerType())
    def mon_name_to_number(name):
        return month_map.get(name)


    df = df.withColumn("Mon_no", mon_name_to_number(col("Month")))
    #df.show()
    window_spec = Window.partitionBy("Region").orderBy("Mon_no")
    result_df = df.withColumn("Cum_Sales", sum("Sales").over(window_spec))\
                .withColumn("Rank", dense_rank().over(window_spec))
    #result_df.select("Rank","Region","Month","Sales","Cum_Sales").show()

    diff_df = result_df.withColumn("prevMon", lag(col("Sales")).over(window_spec))\
                .withColumn("Diff", col("Sales") - col("prevMon"))
    diff_df = diff_df.select("Rank","Region","Month","Sales","prevMon","Cum_Sales",when(col("Diff").isNull(), "0").otherwise(col("Diff")).alias("DiffSales") )
    diff_df.show()
    diff_df.write.json("Output/CumulativeSales.json", mode="overwrite")

    #output
    '''
        +----+------+-------+-----+-------+---------+---------+
        |Rank|Region|  Month|Sales|prevMon|Cum_Sales|DiffSales|
        +----+------+-------+-----+-------+---------+---------+
        |   1|  East|January|  200|   NULL|      200|        0|
        |   2|  East|    Feb|  300|    200|      500|      100|
        |   3|  East|    Mar|  250|    300|      750|      -50|
        |   1| North|    Jan|  300|   NULL|      300|        0|
        |   2| North|    Feb|  300|    300|      600|        0|
        |   3| North|    Mar|  270|    300|      870|      -30|
        |   1| South|  April|  650|   NULL|      650|        0|
        |   2| South|    Jan|  500|    650|     1150|     -150|
        |   3| South|    Feb|  750|    500|     1900|      250|
        |   4| South|    Mar|  650|    750|     2550|     -100|
        |   1|  West|    Jan|  400|   NULL|      400|        0|
        |   2|  West|    Feb|  350|    400|      750|      -50|
        |   3|  West|    Mar|  450|    350|     1200|      100|
        +----+------+-------+-----+-------+---------+---------+
    '''

