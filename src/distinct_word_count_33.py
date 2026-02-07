# pwd : /Users/vatsalsharma/Documents/GIT_REPOs/Databricks/src
# spark-submit distinct_word_count_33.py --text_file_path "../data/gutenberg_books/1342-0.txt"

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import argparse

spark = (
            SparkSession
            .builder
            .getOrCreate()
)

# spark.sparkContext.setLogLevel("WARN")
spark.sparkContext.setLogLevel("ERROR")

def distinct_word_count(file_name):
    results = (
        spark.read.text(file_name)
        .select(F.split(F.col("value"), " ").alias("line"))
        .select(F.explode(F.col("line")).alias("word"))
        .select(F.lower(F.col("word")).alias("word"))
        .select(F.regexp_extract(F.col("word"), "[a-z']*", 0).alias("word"))
        .where(F.col("word") != "")
    )

    return results.select(F.countDistinct("word").alias("distinct_count"))

parser = argparse.ArgumentParser()
parser.add_argument('--text_file_path', help='path of the input text file')
args = parser.parse_args()

# file_name = args.text_file_path
if args.text_file_path:
    file_name = args.text_file_path
else:
    # Default value
    file_name = "../data/gutenberg_books/1342-0.txt"    

# file_name = "../data/gutenberg_books/1342-0.txt"
df = distinct_word_count(file_name)
# df.show()
# +--------------+
# |distinct_count|
# +--------------+
# |          6595|
# +--------------+

data = df.collect()
# print(data) # [Row(distinct_count=6595)]

print(f'Distinct count of words in file {file_name} are {data[0].distinct_count}')
# Distinct count of words in file ../data/gutenberg_books/1342-0.txt are 6595

