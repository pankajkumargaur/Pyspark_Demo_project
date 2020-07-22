from pyspark.sql import SparkSession


spark = SparkSession.\
    builder.master("local[*]").\
    appName("CSV_Load_SQL").getOrCreate()


df = spark.read.format("csv")\
    .option("header", "true")\
    .option("inferSchema", "true")\
    .load("E:\work\Spark_Project_Repos\Spark_Demo_Projects\src\main\datasets\movies.csv")


df.printSchema()
df.show(25, False)