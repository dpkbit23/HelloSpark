from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, count

if __name__ == "__main__":
    spark = SparkSession.builder.appName("count_salary_category").master("local[3]").getOrCreate()
    accounts_data = [(3,108939),(2,12747),(8,87709),(6,91796)]
    accounts_schema = ["account_id","income"]
    account_df = spark.createDataFrame(accounts_data,accounts_schema)
    #account_df.show()

    df_category = account_df.withColumn("category", expr("case when income < 20000 then 'Low Salary' when income > 20000 and income < 50000 then 'Average Salary' else 'High Salary' End"))
    df_category.show()
    #allCategory = [(1,"Low Salary"), (2,"Average Salary"), (3,"High Salary")]
    allCategory = ["Low Salary", "Average Salary", "High Salary"]
    all_schema = ["category"]
    df_allCategory = spark.createDataFrame([(x,) for x in allCategory],all_schema)
    df_allCategory.show()

    df_final = df_category.join(df_allCategory, on="category", how="rightouter")
    #df_final.select("category",count("category")).groupBy("category")
    df_final=df_final.groupBy("category").agg(count("account_id").alias("count"))
    df_final.show()

    '''
    +--------------+-----+
    |      category|count|
    +--------------+-----+
    |    Low Salary|    1|
    |Average Salary|    0|
    |   High Salary|    3|
    +--------------+-----+
    '''