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
# 4. MapReduce : List the top three words that have appeared in the greatest number of years.
####

# Spark SQL

# +-------------+--------+
# |         word|count(1)|
# +-------------+--------+
# |    ATTRIBUTE|      11|
# |approximation|       4|
# |    agast_ADV|       4|
# +-------------+--------+
# only showing top 3 rows

# The above output may look slightly different for you due to ties with other words

f = sc.textFile("gbooks")
data_rdd = f.map(lambda line: (line.split()[0], int(line.split()[1])))

schema = StructType(
    [StructField("word", StringType(), True), StructField("year", IntegerType(), True)]
)

data_df = data_rdd.toDF(schema)
data_df.createOrReplaceTempView("words")

word_count_df = spark.sql(
    """
    SELECT word, COUNT(1) as count
    FROM words
    GROUP BY word
    ORDER BY count DESC
    """
)

word_count_df = word_count_df.withColumnRenamed("count", "count(1)")

word_count_df.show(3)
