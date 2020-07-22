import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

simpleData = (("James", "", "Smith", "36636", "NewYork", 3100), \
              ("Michael", "Rose", "", "40288", "California", 4300), \
              ("Robert", "", "Williams", "42114", "Florida", 1400), \
              ("Maria", "Anne", "Jones", "39192", "Florida", 5500), \
              ("Jen", "Mary", "Brown", "34561", "NewYork", 3000) \
              )
columns = ["firstname", "middlename", "lastname", "id", "location", "salary"]


#df1  = spark.read.format("csv").option("header","true")\
  #  .load(r"C:\Users\Pankaj Gaur\Downloads\result\mydata.csv")



df = spark.createDataFrame(data=simpleData, schema=columns)

#df.coalesce(1).write.format("csv").option("header", "true").save(r"C:\Users\Pankaj Gaur\Downloads\result")


df.printSchema()
df.show()

df.drop(df.firstname) \
    .printSchema()

df.drop("firstname", "middlename", "lastname") \
    .printSchema()


cols = ("firstname", "middlename", "lastname")

df.drop(*cols) \
    .printSchema()


df.withColumnRenamed("firstname","Rfirstname").show()

df.withColumn("test" , lit("computer")).show()

df.withColumn("test" , when(col('salary') > 3000 , "Good Salary").otherwise("Bad salary")) .show()


df.createOrReplaceTempView("TENP")
spark.sql("select *, CASE WHEN SALARY > 3000 THEN 'GOOD SALARY ' ELSE 'BASD SALARY ' END as test from TENP"  ).show()

spark.sql("select firstname , salary  from TENP where salary =3100 ").show()

df.select(col("firstname"),col("salary")).filter(col("salary") == 3100).show()
