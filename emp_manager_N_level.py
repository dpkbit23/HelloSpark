from pyspark.sql import SparkSession
from pyspark.sql.functions import col

if __name__ == "__main__":
    spark = SparkSession.builder.master("local[*]").getOrCreate()
    data = [(1, 'A',2),(2, 'B',''),(3, 'C',1),(4, 'D',2),(5, 'E',1)]
    schema =["empId", "name","manager"]
    df = spark.createDataFrame(data,schema)
    df.show()
    df.createOrReplaceTempView("emp")
    sql_df = spark.sql("select e.empId as employeeId ,e.name as employee, m.name as Manager, sm.name as Senior_Manager from emp e left outer join emp m on m.empId=e.manager left outer join emp sm on m.manager = sm.empId")
    e = df.alias('e')
    m = df.alias('m')
    sm  =df.alias("sm")
    emp_manager_df = e.join(m, col('m.empId')==col('e.manager'), how='left' ).join(sm, col('m.manager')==col('sm.empId'), how='left')
    emp_manager_df.select(col('e.empId').alias('employeeId'), col('e.name').alias('employee'), col('m.name').alias('Manager'), col('sm.name').alias('Senior_Manager')).show()
    sql_df.show()
