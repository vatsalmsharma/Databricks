from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = (
            SparkSession
            .builder
            .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# Excercise 3.4
# Script to return a sample of five words that appear only once in Jane Austen’s Pride and Prejudice.
results = (
    spark.read.text("../data/gutenberg_books/1342-0.txt")
    .select(F.split(F.col("value"), " ").alias("line"))
    .select(F.explode(F.col("line")).alias("word"))
    .select(F.lower(F.col("word")).alias("word"))
    .select(F.regexp_extract(F.col("word"), "[a-z']*", 0).alias("word"))
    .where(F.col("word") != "")
    .groupby("word")
    .count()
)

(results
    .select(F.col("word"), F.col("count"))
    .where(F.col("count") == 1)
    .show(5)
)

# Excercise 3.5
# return the top five most popular first letters (keep only the first letter of each word).
results = (
    spark.read.text("../data/gutenberg_books/1342-0.txt")
    .select(F.split(F.col("value"), " ").alias("line"))
    .select(F.explode(F.col("line")).alias("word"))
    .select(F.lower(F.col("word")).alias("word"))
    .select(F.regexp_extract(F.col("word"), "[a-z']*", 0).alias("word"))
    .where(F.col("word") != "")
    .select(F.substring(F.col("word"),1,1).alias("first_letter"))
    .groupBy("first_letter")
    .count()
)

results.orderBy("count", ascending=False).show(5)

# +------------+-----+
# |first_letter|count|
# +------------+-----+
# |           t|16101|
# |           a|13684|
# |           h|10419|
# |           w| 9091|
# |           s| 8791|
# +------------+-----+

# Compute the number of words starting with a consonant or a vowel. 
vowel = ['a', 'e', 'i', 'o', 'u']

(results
    .withColumn(
        "type",
        F.when(F.col("first_letter").isin(vowel), "vowel")
        .otherwise("consonant")
    )
    .select(F.col("type"), F.col("count"))
    .groupBy("type")
    .agg(F.sum(F.col("count")).alias("total_count")
        , F.count(F.col("count")).alias("type_count")
        )
    .show(5)
)

# +---------+----------+
# |     type|sum(count)|
# +---------+----------+
# |consonant|     88653|
# |    vowel|     33522|
# +---------+----------+

# +---------+-----------+----------+
# |     type|total_count|type_count|
# +---------+-----------+----------+
# |consonant|      88653|        20|
# |    vowel|      33522|         5|
# +---------+-----------+----------+