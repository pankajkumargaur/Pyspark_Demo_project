from pyspark.sql import SparkSession

from pyspark.sql import *

spark = SparkSession \
    .builder \
    .appName("Load Data") \
    .master("local[*]") \
    .getOrCreate()

def getData(x):
    fields = x.split(',')
    return (fields[1], int(fields[2]))

scContext =  spark.sparkContext

myRDD = scContext.textFile("E:\work\datasets\Customer.csv")


rddExHeader = myRDD.filter(lambda rec:  not rec.startswith('Id'))

# val rddExHeader =  myRDD.filter(lambda rec :  !rec.startsWith("Id"))


result = rddExHeader.map(getData).\
    filter(lambda t: t[1] > 21)


#rddExHeader.map(lambda rec: (rec.split(",")[0],rec.split(",")[1])  )

# Print Result
for x in result.collect():
    print(x)

#result.saveAsTextFile("E:\work\datasets\mera_output")

#res.foreach(print)






