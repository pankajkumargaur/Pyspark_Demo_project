from pyspark.sql import *

import sys

spark = SparkSession \
    .builder \
    .appName("word count") \
    .master("local[*]") \
    .getOrCreate()


  # E:\work\Spark_Project_Repos\Spark_Demo_Projects\src\main\datasets\myfile.txt
scContext =  spark.sparkContext

rdd = scContext.textFile(sys.argv[0])

resultRDD  = rdd.flatMap(lambda line: line.split(" "))\
    .map(lambda word: (word, 1))\
    .reduceByKey(lambda x, y : x + y)\
    .map(lambda t: (t[1], t[0]))\
    .sortByKey(False) \

output = resultRDD.collect()

for x in output:
    print ( x[1], x[0])






