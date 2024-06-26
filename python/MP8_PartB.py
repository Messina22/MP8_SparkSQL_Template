from pyspark import SparkContext
from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from pyspark.sql.types import StringType, IntegerType
from pyspark.sql import SparkSession

sc = SparkContext()
spark = SparkSession.builder.getOrCreate()

####
# 1. Setup : Write a function to load it in an RDD & DataFrame
####

# RDD API
# Columns:
# 0: word (string), 1: year (int), 2: frequency (int), 3: books (int)


# Spark SQL - DataFrame API

####
# 2. Counting : How many lines does the file contains? Answer this question via both RDD api & #Spark SQL
####

# Spark SQL

# sqlContext.sql(query).show() or df.show()
# +--------+
# |count(1)|
# +--------+
# |   50013|
# +--------+

f = sc.textFile("gbooks")
data_rdd = f.map(lambda line: [x for x in line.split()])

schema = StructType([StructField("count(1)", StringType(), True)])

data_df = sc.parallelize([(data_rdd.count(),)]).toDF(["count(1)"])
data_df.show()
