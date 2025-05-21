from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

if __name__ == "__main__":
    spark = SparkSession.builder.master("local[*]").appName("customer_cdc").getOrCreate()
    cust_schema = ["customer_id", "name", "email", "city"]

    prev_data = [(1, "Derek", "derek@mail.com", "New York"),
                 (2, "Zara", "zara@mail.com", "Chicago"),
                 (3, "Marco", "marco@mail.com", "Seattle")]

    current_data = [(1, "Derek", "derek@mail.com", "New York"),
                    (2, "Zara", "zara123@mail.com", "Chicago"),
                     (4, "Diana", "diana@mail.com", "Los Angeles")]

    prev_cust_df = spark.createDataFrame(prev_data, cust_schema)
    current_cust_df = spark.createDataFrame(current_data, cust_schema)

    #new customer
    new_cust_df = (current_cust_df.alias("curr").join(prev_cust_df.alias("prev"), on = "customer_id", how="left_anti" )
                   .select("*",lit("Added").alias("Change_type")))

    #deleted customer
    del_cust_df = (prev_cust_df.alias("prev").join(current_cust_df.alias("curr"), on ="customer_id", how ="left_anti")
                   .select("*",lit("Deleted").alias("Change_type")))

    #updated customer
    common_cust_df = current_cust_df.alias("curr").join(prev_cust_df.alias("prev"), on = "customer_id", how="inner" )
    upd_cust_df = common_cust_df.filter(
        (col("curr.customer_id") != col("prev.customer_id")) |
        (col("curr.email") != col("prev.email")) |
        (col("curr.city") != col("prev.city")) |
        (col("curr.name") != col("prev.name"))
    ).select("curr.*",lit("Modified").alias("Change_type"))

    final_cust_df = new_cust_df.union(del_cust_df).union(upd_cust_df)
    final_cust_df.show()

    '''
    +-----------+-----+----------------+-----------+-----------+
    |customer_id| name|           email|       city|Change_type|
    +-----------+-----+----------------+-----------+-----------+
    |          4|Diana|  diana@mail.com|Los Angeles|      Added|
    |          3|Marco|  marco@mail.com|    Seattle|    Deleted|
    |          2| Zara|zara123@mail.com|    Chicago|   Modified|
    +-----------+-----+----------------+-----------+-----------+
    '''