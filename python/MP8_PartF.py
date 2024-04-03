from pyspark.sql.functions import col, lag
from pyspark import SparkContext
from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from pyspark.sql.types import StringType, IntegerType
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import *

sc = SparkContext()
spark = SparkSession.builder.getOrCreate()

####
# 1. Setup : Write a function to load it in an RDD & DataFrame
####

# RDD API
# Columns:
# 0: word (string), 1: year (int), 2: frequency (int), 3: books (int)

f = sc.textFile("gbooks")
data_rdd = f.map(
    lambda line: (line.split()[0], int(line.split()[1]), int(line.split()[2]))
)

schema = StructType(
    [
        StructField("word", StringType(), True),
        StructField("year", IntegerType(), True),
        StructField("frequency", IntegerType(), True),
    ]
)

df = data_rdd.toDF(schema)

###
# 2. Frequency Increase : analyze the frequency increase of words starting from the year 1500 to the year 2000
###
# Spark SQL - DataFrame API

new_df = df.filter(df.year >= 1500).filter(df.year <= 2000)

window = Window.partitionBy("word").orderBy("year")

df_lag = new_df.withColumn("lag", lag("frequency", -1).over(window))
df_word_increase = df_lag.groupBy("word").agg(sum("lag").alias("total_increase"))

df_word_increase.orderBy(col("total_increase").desc()).show()


# df_word_increase.show()
