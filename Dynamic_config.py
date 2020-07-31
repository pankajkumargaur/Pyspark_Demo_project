import argparse
import configparser
from pyspark.sql import SparkSession

from pyspark.sql import *

if __name__ == '__main__':
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument('--user', action="store")
    parser.add_argument('--password', action="store")
    parser.add_argument('--file_path', action="store")
    #parser.add_argument('--test', action="store", required=True)

    args = parser.parse_args()

    print(args)

    #r"E:\work\Spark_Project_Repos\pyspark_trainig\appConfig.properties"
    config = configparser.RawConfigParser()
    config.read(args.file_path)


    details_dict = dict(config.items('Local'))
    print(details_dict)
    #3print(details_dict.get("path2", 'NA'))



    spark = SparkSession \
        .builder \
        .appName("Load Data") \
        .master("local[*]") \
        .getOrCreate()

    fileformat =  details_dict.get("fileforamt")
    filePath = details_dict.get("filename")
    jdbcURL  = details_dict.get("url")
    print("jdbcURL URL is " , jdbcURL)

    df = spark.read.format(fileformat).load(filePath)

    df.printSchema()
    df.show()




