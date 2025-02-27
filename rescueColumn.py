from itertools import chain

from pyspark.sql import SparkSession
from pyspark.sql.functions import create_map, lit, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, MapType

if __name__ == "__main__":

    spark =  SparkSession.builder.master("local[3]").appName("rescueColumns").getOrCreate()

    '''
    schema = StructType([
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True)
    ])

    df =  spark.read.schema(schema).json("data/people.json")
    df_rescue = df.selectExpr("name","age","_rescued_data")
    df_rescue.show()
    '''
    expected_schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True)
    ])

    # Sample Data with Unexpected Columns
    json_data = [
        {"id": 1, "name": "John", "age": "30", "city": "New York"},
        {"id": 2, "name": "Steve", "gender": "Male", "country": "USA"}
    ]

    # Convert to DataFrame without enforcing schema (to retain all columns)
    df = spark.createDataFrame(json_data)

    # Get the list of expected columns
    expected_cols = set([field.name for field in expected_schema])

    # Identify unexpected columns
    unexpected_cols = [col(c).cast(StringType()) for c in df.columns if c not in expected_cols]

    # Create a map column to capture unexpected columns
    if unexpected_cols:
        rescued_data_col = create_map(list(chain(*[(lit(c), col(c)) for c in df.columns if c not in expected_cols])))
        df = df.withColumn("_rescued_data", rescued_data_col)
    else:
        df = df.withColumn("_rescued_data", lit(None).cast(MapType(StringType(), StringType())))

    # Select only expected columns + rescued column
    final_df = df.select(*expected_cols, "_rescued_data")

    # Show Output
    final_df.show(truncate=False)
    final_df.printSchema()

