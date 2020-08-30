from pyspark.SparkSession import *
from pyspark.conf import SparlConf
from pyspark.sql import SpqrkSession



spark = SparkSession.builder \
		.master("local") \
		.appName("Reading csv data into DataFrame") \
		.config("spark.some.config.option", "some-value") \
		.getOrCreate()
		
		
		
DF=spark.read.csv("D:\Sample_Data/Bhav.csv")

DF.show()
		