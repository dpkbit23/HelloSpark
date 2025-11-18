
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.functions import col, window

if __name__ == "__main__":
    spark = SparkSession.builder.master("local[3]").getOrCreate()

    data = [
        (101, "2025-01-01", "Absent"),
        (101, "2025-01-02", "Absent"),
        (101, "2025-01-03", "Present"),
        (101, "2025-01-04", "Absent"),
        (101, "2025-01-05", "Absent"),
        (101, "2025-01-06", "Absent"),
        (101, "2025-01-07", "Present"),
        (102, "2025-01-01", "Present"),
        (102, "2025-01-02", "Absent"),
        (102, "2025-01-03", "Absent"),
        (102, "2025-01-04", "Present")
    ]
    df = spark.createDataFrame(data, ["student_id","Attendance_dt","Status"])
    df_abs = df.filter(col("Status")=="Absent")
    w = Window.partitionBy("student_id").orderBy("Attendance_dt")

    df_preDt = df_abs.withColumn("Prev_dt", F.lag("Attendance_dt").over(w))
    df3 = df_preDt.withColumn(
        "is_new_group",
        F.when(F.date_sub("Attendance_dt", 1) != F.col("Prev_dt"), 1).otherwise(0)
    )
    df4 = df3.withColumn(
        "group_id",
        F.sum("is_new_group").over(w)
    )
    result_df = df4.groupBy("student_id", "group_id").agg(F.min("Attendance_dt").alias("Start_date"), F.max("Attendance_dt").alias("End_date"), F.count("*").alias("Consecutiove_days")).orderBy("Student_id","Start_date")   #select("student_id",)
    result_df.show()