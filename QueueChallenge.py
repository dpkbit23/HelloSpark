from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import  sum, col, desc

if __name__ == "__main__":
    spark = SparkSession.builder.appName("QueueChallenge").master("local[3]").getOrCreate()

    data = [
        (101, "Derek", 210, 7),
        (102, "Lena", 185, 2),
        (103, "Marco", 240, 9),
        (104, "Tina", 195, 1),
        (105, "Eli", 260, 4),
        (106, "Zara", 230, 10),
        (107, "Nathan", 275, 3),
        (108, "Priya", 205, 6),
        (109, "Yusuf", 250, 5),
        (110, "Mei", 190, 8)
    ]

    columns = ["person_id", "person_name", "weight", "turn"]
    df = spark.createDataFrame(data, columns)
    window_spec = Window.orderBy("turn")

    LastMan_df= df.withColumn("total_weight", sum("weight").over(window_spec))
    LastMan_df.show()
    LastMan_df=  LastMan_df.filter(col("total_weight") <= 1000).orderBy(desc("total_weight")).limit(1).select("person_name")
    LastMan_df.show()

    '''
    output
    +-----------+
    |person_name|
    +-----------+
    |        Eli|
    +-----------+
    '''
