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

f = sc.textFile("gbooks")
data_rdd = f.map(lambda line: (line.split()[0], int(line.split()[1])))

schema = StructType(
    [StructField("word", StringType(), True), StructField("year", IntegerType(), True)]
)

df = data_rdd.toDF(schema)


####
# 5. Joining : The following program construct a new dataframe out of 'df' with a much smaller size.
####

df2 = df.select("word", "year").distinct().limit(100)
df2.createOrReplaceTempView("gbooks2")

# Now we are going to perform a JOIN operation on 'df2'. Do a self-join on 'df2' in lines with the same #'count1' values and see how many lines this JOIN could produce. Answer this question via Spark SQL API

joined = spark.sql(
    """
    SELECT a.word AS word1, b.word AS word2, a.year AS year
    FROM gbooks2 AS a
    JOIN gbooks2 AS b
    ON a.year = b.year
    """
)

print(joined.count())

# Spark SQL API

# output: 166
