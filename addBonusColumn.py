from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, round, dense_rank

if __name__ == "__main__":
    spark = SparkSession.builder.appName("addBonusCol").master("local[3]").getOrCreate()

    data = [(1, "John Doe", "IT", 80000, "2021-01-15"),
            (2, "Jane Smith", "HR", 70000, "2010-05-22"),
            (3, "Robert Brown", "IT", 85000, "2019-11-03"),
            (4, "Emily Davis", "Finance", 90000, "2022-07-19"),
            (5, "Michael Johnson", "HR", 75000, "2023-03-11"),
            (6, "Michael Johnson", "HR", 76000, "2023-03-11")]

    columns = ["EmpID", "EmpName", "Dept", "Salary", "DoJ"]

    df_sal = spark.createDataFrame(data, columns)
    df_bonus = df_sal.withColumn("bonus", round(col("Salary")* 0.15,3))
    df_bonus.show()

    window_spec = Window.partitionBy(col("Dept")).orderBy(col("Salary").desc())

    df_second_sal = df_sal.withColumn("rank", dense_rank().over(window_spec))
    df_second_sal.show()
    #filter out rank 2 and 1 from the complete list
    df1 = df_second_sal.filter((col("rank") == 2) | (col("rank") == 1)).orderBy(col("Dept"),col("rank").desc())
    df1.show()
    #again create a col to rank only 1st and 2nd salary in desc order so that rank 2 becomes rank 1
    window_spec1 = Window.partitionBy(col("dept")).orderBy(col("rank").desc())
    df2 = df1.withColumn("secSal", dense_rank().over(window_spec1))
    df2.show()
    # filter rank 1 i.e 2nd rank after creating the rank second time
    df3 = df2.filter(col("secSal") == 1 )
    df3.select("EmpID","EmpName","Dept","Salary","DoJ").show()

    '''df = df_second_sal.filter((col("rank") == 2) |
           ((col("rank") == 1) & (~df_second_sal.Dept.isin(
               df_second_sal.filter(col("rank") == 2).select(col("Dept")).rdd.flatMap(
                   lambda x: x)
           ))))
    #df_sal.show()
    df.show()
    
   
    # Sample Data (Employee, Department, Salary)
    data = [
        ("Alice", "HR", 5000),
        ("Bob", "HR", 7000),
        ("Charlie", "IT", 6000),
        ("David", "IT", 8000),
        ("Eve", "IT", 8000),  # Duplicate highest salary
        ("Frank", "Sales", 4000)  # Only one employee in Sales
    ]

    columns = ["Name", "Department", "Salary"]

    # Create DataFrame
    df = spark.createDataFrame(data, columns)

    # Define Window Spec (Partition by Department, Order by Salary Desc)
    windowSpec = Window.partitionBy("Department").orderBy(col("Salary").desc())

    # Rank Salaries within Each Department
    df_ranked = df.withColumn("rank", dense_rank().over(windowSpec))

    # Filter for second highest (rank=2), or highest if rank=1 and no rank=2 exists
    df_second_highest = df_ranked.filter((col("rank") == 2) |
                                         ((col("rank") == 1) & (~df_ranked.Department.isin(
                                             df_ranked.filter(col("rank") == 2).select("Department").rdd.flatMap(
                                                 lambda x: x).collect()
                                         ))))

    # Show Result
    df_second_highest.select("Name", "Department", "Salary").show()
    '''

    '''
    Bonus
    +-----+---------------+-------+------+----------+-------+
    |EmpID|        EmpName|   Dept|Salary|       DoJ|  bonus|
    +-----+---------------+-------+------+----------+-------+
    |    1|       John Doe|     IT| 80000|2021-01-15|12000.0|
    |    2|     Jane Smith|     HR| 70000|2010-05-22|10500.0|
    |    3|   Robert Brown|     IT| 85000|2019-11-03|12750.0|
    |    4|    Emily Davis|Finance| 90000|2022-07-19|13500.0|
    |    5|Michael Johnson|     HR| 75000|2023-03-11|11250.0|
    |    6|Michael Johnson|     HR| 76000|2023-03-11|11400.0|
    +-----+---------------+-------+------+----------+-------+
        
    second highest salary of each dept, if 2nd does not exists then first highest salary
    +-----+---------------+-------+------+----------+
    |EmpID|        EmpName|   Dept|Salary|       DoJ|
    +-----+---------------+-------+------+----------+
    |    4|    Emily Davis|Finance| 90000|2022-07-19|
    |    5|Michael Johnson|     HR| 75000|2023-03-11|
    |    1|       John Doe|     IT| 80000|2021-01-15|
    +-----+---------------+-------+------+----------+
    '''