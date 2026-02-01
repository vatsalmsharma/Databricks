# Databricks notebook source
from pyspark.sql.functions import input_file_name, current_timestamp, lit, col
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

# COMMAND ----------

raw_cust_folder = "/Volumes/workspace/zions_schema/cust_inbound"
raw_comm_folder = "/Volumes/workspace/zions_schema/cust_comm_inbound"

# COMMAND ----------

cust_schema = StructType([
    StructField("cust_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("event_ts", StringType(), True), 
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("addr_city", StringType(), True),
    StructField("addr_state", StringType(), True),
    StructField("addr_postal", StringType(), True),
])

# COMMAND ----------

comm_schema = StructType([
    StructField("cust_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("event_ts", StringType(), True),
    StructField("reach_via", StringType(), True)
])

# COMMAND ----------

df_cust_raw = (spark.read
          .option("header", True)
          .schema(schema)
          .csv(raw_cust_folder)
          .withColumn("ingest_ts", current_timestamp())
#          .withColumn("source_file", input_file_name())
        .withColumn("source_file", col("_metadata.file_name"))
         )

display(df_cust_raw.limit(20))

# COMMAND ----------

df_comm_raw = (spark.read
               .option("header", True)
               .schema(comm_schema)
               .csv(raw_comm_folder)
               .withColumn("ingest_ts", current_timestamp())
               .withColumn("source_file", col("_metadata.file_name"))
              )

# COMMAND ----------

bronze_cust = 'zions_schema.bronze_cust'
bronze_comm = 'zions_schema.bronze_comm'


# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS bronze_cust
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS bronze_comm
""")

# COMMAND ----------

(df_cust_raw.write
 .mode("append")
 .option("mergeSchema", "true")
 .saveAsTable(bronze_cust))

# COMMAND ----------

(df_comm_raw.write
 .mode("append")
 .option("mergeSchema", "true")
 .saveAsTable(bronze_comm))

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from zions_schema.bronze_cust 
# MAGIC order by cust_id
# MAGIC limit 20

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from zions_schema.bronze_comm 
# MAGIC order by cust_id
# MAGIC limit 20

# COMMAND ----------

