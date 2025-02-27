from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import to_date, col, window, avg

if __name__ == "__main__":
    spark = SparkSession.builder.appName("MovingAvg").master("local[3]").getOrCreate()

    data = [
                (1, "2024-01-01", 10),
                (2, "2024-01-02", 20),
                (3, "2024-01-03", 30),
                (4, "2024-01-04", 40),
                (5, "2024-01-05", 50)
            ]

    column = ["id","date","value"]
    df = spark.createDataFrame(data, column)
    df = df.withColumn("date", to_date(col("date")))

    #-2,0 = last 2 rows + current rows
    window_spec = Window.orderBy(col("date")).rowsBetween(-2,0)

    #moving average
    df = df.withColumn("movingAvg", avg("value").over(window_spec))
    df.show()

    '''
    output
    +---+----------+-----+---------+
    | id|      date|value|movingAvg|
    +---+----------+-----+---------+
    |  1|2024-01-01|   10|     10.0|
    |  2|2024-01-02|   20|     15.0|
    |  3|2024-01-03|   30|     20.0|
    |  4|2024-01-04|   40|     30.0|
    |  5|2024-01-05|   50|     40.0|
    +---+----------+-----+---------+
    '''