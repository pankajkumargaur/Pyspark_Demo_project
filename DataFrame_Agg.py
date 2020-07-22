from pyspark.sql import SparkSession

from pyspark.sql import *
from pyspark.sql.functions import *

simpleData = [("James","Sales","NY",90000,34,10000),
    ("Michael","Sales","NY",86000,56,20000),
    ("Robert","Sales","CA",81000,30,23000),
    ("Maria","Finance","CA",90000,24,23000),
    ("Raman","Finance","CA",99000,40,24000),
    ("Scott","Finance","NY",83000,36,19000),
    ("Jen","Finance","NY",79000,53,15000),
    ("Jeff","Marketing","CA",80000,25,18000),
    ("Kumar","Marketing","NY",91000,50,21000)
  ]

spark = SparkSession \
    .builder \
    .appName("CSV_Load_SQL") \
    .master("local[*]") \
    .getOrCreate()

schema = ["employee_name","department","state","salary","age","bonus"]

df = spark.createDataFrame(data=simpleData, schema = schema)

df.printSchema()
df.show()


df.createOrReplaceTempView("temp")

spark.sql("select department , sum(salary) as Total from temp group by department  ").show()


df.groupBy("department").sum("salary").show()


df.groupBy("department").count().show()

# Calculate the minimum salary of each department
df.groupBy("department").min("salary")


df.groupBy("department").avg( "salary")


df.groupBy("department").mean( "salary")



df.groupBy("department").mean( "salary")



df.groupBy("department","state") \
    .sum("salary","bonus") \
    .show()


df.groupBy("department") \
    .agg(sum("salary").alias("sum_salary"), \
         avg("salary").alias("avg_salary"), \
         sum("bonus").alias("sum_bonus"), \
         max("bonus").alias("max_bonus"), \
         mean("salary").alias("mean_salary"), \
     ).show(truncate=False)



df.groupBy("department") \
    .agg(sum("salary").alias("sum_salary"), \
      avg("salary").alias("avg_salary"), \
      sum("bonus").alias("sum_bonus"), \
      max("bonus").alias("max_bonus")).where(col("sum_bonus") >= 50000) \
     .show(truncate=False)


