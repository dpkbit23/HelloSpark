from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit

if __name__ == "__main__":
    spark = SparkSession.builder.appName("footballMatch").getOrCreate()
    teams = [
        (10, "Brazil"),(20, "Argentina"),(30, "France"),(40, "Germany"),(50, "Italy")
    ]
    matches = [
        (1, 10, 20, 3, 0), (2, 30, 10, 2, 2),(3, 10, 50, 5, 1),(4, 20, 30, 1, 0),(5, 50, 30, 1, 0)
    ]

    team_df = spark.createDataFrame(teams,["team_id", "team_name"])
    match_df = spark.createDataFrame(matches, ["match_id", "host_team","guest_team","host_goal","guest_goal"])

    host_df = match_df.select(
        col("host_team"),col("host_goal"), col("guest_goal")
    ).withColumn("points", when(col("host_goal") > col("guest_goal"), lit(3))
                                    .when(col("host_goal") == col("guest_goal"), lit(1))
                                    .otherwise(lit(0)) )
    guest_df = match_df.withColumn("points", when(col("guest_goal") > col("host_goal"), lit(3))
                                                    .when(col("host_goal") == col("guest_goal"), lit(1))
                                                    .otherwise(lit(0)))
    guest_df = guest_df.select(col("guest_team").alias("team"),col("guest_goal").alias("host_goal"),col("host_goal").alias("guest_goal"), "points")

    all_score_df = host_df.union(guest_df)
    all_score_df = all_score_df.select("host_team", "points").groupBy("host_team").sum("points")
    final_table = all_score_df.join(team_df, team_df["team_id"]== all_score_df["host_team"], "right").fillna(0)
    final_table.select(col("team_name").alias("Team"), col("sum(points)").alias("Points")).orderBy(col("Points").desc()).show()


