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
# 3. Filtering : Count the number of appearances of word 'ATTRIBUTE'
####

# Spark SQL

# +--------+
# |count(1)|
# +--------+
# |      11|
# +--------+

f = sc.textFile("gbooks")
data_rdd = f.map(lambda line: (line.split()[0], int(line.split()[1])))

schema = StructType(
    [StructField("word", StringType(), True), StructField("year", IntegerType(), True)]
)

data_df = data_rdd.toDF(schema)
count = data_df.filter(data_df.word == "ATTRIBUTE").count()

new_df = sc.parallelize([(count,)]).toDF(["count(1)"])
new_df.show()
