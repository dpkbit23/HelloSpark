from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import window, col, row_number, dense_rank

if __name__ == "__main__":
    spark =  SparkSession.builder.master("local[*]").appName("CustRecentPurchase").getOrCreate()

    data = [ (1, "Laptop", "2025-01-01"), (1, "Mouse", "2025-04-04"),(1, "Scanner", "2025-04-05"), (2, "Keyboard", "2024-08-02"), (2, "Monitor", "2024-08-03") ]

    columns = ["customer_id", "product", "purchase_date"]

    data_df = spark.createDataFrame(data, columns)
    data_df.show()

    window_spec = Window.partitionBy("customer_id").orderBy(col("purchase_date").desc())

    purchase_df = ((data_df.withColumn("rank", dense_rank().over(window_spec))
          .filter(col('rank')==1))
          .drop("rank"))

    purchase_df.show()

    '''
    +-----------+-------+-------------+
    |customer_id|product|purchase_date|
    +-----------+-------+-------------+
    |          1|Scanner|   2025-04-05|
    |          2|Monitor|   2024-08-03|
    +-----------+-------+-------------+
    '''