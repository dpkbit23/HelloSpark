from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import regexp_replace, split, explode, col, posexplode, row_number, expr, floor

if __name__ == "__main__":
    spark = SparkSession.builder.appName("singleRowText").master("local[*]").getOrCreate()

    raw_df = spark.read.text("data/singleRowPipe.txt")
    raw_df = raw_df.toDF("rawData")
    #raw_df.show(truncate=False)

    # Split the raw data into an array using '|'
    df_split = raw_df.withColumn("splitdata", split(col("rawData"), "\\|"))

    df_split.show()

    #Explode the array into multiple rows
    df_exploded = df_split.selectExpr("posexplode(splitdata) as (posNo, value)")
    df_exploded.show()

    #Assign row numbers to each exploded row
    window_spec = Window.orderBy("posNo")
    df_with_row_number = df_exploded.withColumn("rowNum", row_number().over(window_spec))
    df_with_row_number.show()

    #Create a group column for every 4th elements
    df_with_group = df_with_row_number.withColumn("group", floor((col("rowNum") - 1) / 4))
    df_with_group.show()

    #Add a position index within each group
    df_with_group = df_with_group.withColumn("pos_in_group", (col("rowNum") - 1) % 4)
    df_with_group.show()

    #Pivot and convert cols to rows
    df_grouped = df_with_group.groupBy("group").pivot("pos_in_group").agg(expr("first(value)"))
    df_grouped.show()

    #Rename columns to original col names
    df_final = df_grouped.selectExpr(
        "`0` as Name",
            "`1` as Dept",
            "`2` as Exp",
            "`3` as Tech"
    )

    # Show final output
    df_final.show(truncate=False)

    '''
    input
    John|BE|12|BigData|Alice|Btech|11|AWS|Ama|MCA|14|Spark|Isla|ME|9|Cloud|Olivia|BE|11|SQL
    
    output
    +------+-----+---+-------+
    |Name  |Dept |Exp|Tech   |
    +------+-----+---+-------+
    |John  |BE   |12 |BigData|
    |Alice |Btech|11 |AWS    |
    |Ama   |MCA  |14 |Spark  |
    |Isla  |ME   |9  |Cloud  |
    |Olivia|BE   |11 |SQL    |
    +------+-----+---+-------+
    '''