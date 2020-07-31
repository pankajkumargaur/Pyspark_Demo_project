from pyspark.sql import SparkSession

from pyspark.sql import *

import sys

spark = SparkSession \
    .builder \
    .appName("CSV_Load_SQL") \
    .master("local[*]") \
    .getOrCreate()

df = spark.read.format("csv").load(sys.argv[1])

df.printSchema()
df.show()



def add(x,y):
    return x+y
