# Databricks notebook source
from pyspark.sql.functions import col, trim, lower, when, to_timestamp, lit
from pyspark.sql.types import IntegerType, TimestampType
import pyspark.sql.functions as F

# COMMAND ----------

bronze_cust = 'zions_schema.bronze_cust'
bronze_comm = 'zions_schema.bronze_comm'

silver_cust = 'zions_schema.silver_cust'
silver_comm = 'zions_schema.silver_comm'

quarantine_cust = 'zions_schema.quarantine_cust'
quarantine_comm = 'zions_schema.quarantine_comm'

# COMMAND ----------

df_bronze_cust = spark.table(bronze_cust)
df_bronze_comm = spark.table(bronze_comm)


# COMMAND ----------

df_cust = (df_bronze_cust
           .withColumn("cust_id_int", col("cust_id").cast("int"))
           .withColumn("event_ts_parsed", to_timestamp(col("event_ts"), "yyyy-MM-dd'T'HH:mm:ssX"))
           .withColumn("first_name", lower(trim(col("first_name"))))
           .withColumn("last_name", lower(trim(col("last_name"))))
           .withColumn("email", lower(trim(col("email"))))
           .withColumn("phone", trim(col("phone")))
           .withColumn("addr_city", trim(col("addr_city")))
           .withColumn("addr_state", trim(col("addr_state")))
           .withColumn("addr_postal", trim(col("addr_postal")))
          )

# COMMAND ----------

dq_reasons = F.array()

dq_reasons = F.array_union(dq_reasons,
                           F.when(col("cust_id_int").isNull(), F.array(F.lit("MISSING_CUST_ID"))).otherwise(F.array()))
dq_reasons = F.array_union(dq_reasons,
                           F.when(col("event_ts_parsed").isNull(), F.array(F.lit("MISSING_EVENT_TS"))).otherwise(F.array()))
dq_reasons = F.array_union(dq_reasons,
                           F.when((col("addr_state").isNotNull()) & (F.length(col("addr_state")) != 2),
                                  F.array(F.lit("INVALID_STATE_LEN"))).otherwise(F.array()))

df_cust = df_cust.withColumn("dq_reasons_arr", dq_reasons).withColumn("dq_reasons",
                                                                    F.when(F.size(col("dq_reasons_arr"))>0,
                                                                           F.array_join(col("dq_reasons_arr"), ",")).otherwise(None)
                                                                   )

# COMMAND ----------

df_cust_valid = df_cust.filter(col("dq_reasons").isNull())
df_cust_invalid = df_cust.filter(col("dq_reasons").isNotNull())

# COMMAND ----------

silver_cols = ["cust_id_int","event_type","event_ts_parsed","first_name","last_name","email","phone",
               "addr_city","addr_state","addr_postal","ingest_ts","source_file"]

df_cust_valid_final = (df_cust_valid
                       .select([col(c).alias(c.replace("cust_id_int","cust_id") if c=="cust_id_int" else c) for c in silver_cols])
                      )

# COMMAND ----------

(df_cust_valid_final.write
 .mode("append")
 .option("mergeSchema", "true")
 .saveAsTable(silver_cust))

# COMMAND ----------

(df_cust_invalid
 .select("*")
 .withColumnRenamed("dq_reasons","dq_failure_reasons")
 .write.mode("append").option("mergeSchema","true")
 .saveAsTable(quarantine_cust)
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from zions_schema.silver_cust ;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from zions_schema.quarantine_cust

# COMMAND ----------

# Comm data handling 
df_bronze_comm = spark.table(bronze_comm)

# COMMAND ----------

df_comm = (df_bronze_comm
           .withColumn("cust_id_int", col("cust_id").cast("int"))
           .withColumn("event_ts_parsed", to_timestamp(col("event_ts"), "yyyy-MM-dd'T'HH:mm:ssX"))
           .withColumn("reach_via_norm", lower(trim(col("reach_via"))))
          )

# COMMAND ----------

dq_reasons_comm = F.array()
dq_reasons_comm = F.array_union(dq_reasons_comm,
                                F.when(col("cust_id_int").isNull(), F.array(F.lit("MISSING_CUST_ID"))).otherwise(F.array()))
dq_reasons_comm = F.array_union(dq_reasons_comm,
                                F.when(col("event_ts_parsed").isNull(), F.array(F.lit("MISSING_EVENT_TS"))).otherwise(F.array()))

df_comm = df_comm.withColumn("dq_reasons_arr", dq_reasons_comm).withColumn("dq_reasons",
                                                                    F.when(F.size(col("dq_reasons_arr"))>0,
                                                                           F.array_join(col("dq_reasons_arr"), ",")).otherwise(None)
                                                                   )

# COMMAND ----------

df_comm_dq_ok = df_comm.filter(col("dq_reasons").isNull())
df_comm_dq_bad = df_comm.filter(col("dq_reasons").isNotNull())

# COMMAND ----------

silver_cust_ids = (spark.table(silver_cust)
                   .select(col("cust_id").alias("sc_cust_id"))
                   .distinct())

# COMMAND ----------

# %sql
# select * from zions_schema.silver_cust
# order by cust_id;

# COMMAND ----------

# %sql
# select * from zions_schema.quarantine_cust
# order by cust_id;

# COMMAND ----------

df_comm_enriched = (df_comm_dq_ok.join(silver_cust_ids,
                                      df_comm_dq_ok.cust_id_int == silver_cust_ids.sc_cust_id,
                                      how="left"))

# COMMAND ----------

df_comm_enriched = df_comm_enriched.withColumn("ref_failure",
                                               F.when(col("sc_cust_id").isNull(), F.lit("FK_MISSING_CUSTOMER")).otherwise(None))


# COMMAND ----------

df_comm_ref_ok = df_comm_enriched.filter(col("ref_failure").isNull())
df_comm_ref_bad = df_comm_enriched.filter(col("ref_failure").isNotNull())

# COMMAND ----------

df_comm_valid_final = (df_comm_ref_ok
                       .select(
                           col("cust_id_int").alias("cust_id"),
                           col("event_type"),
                           col("event_ts_parsed").alias("event_ts"),
                           col("reach_via_norm").alias("reach_via"),
                           col("ingest_ts"),
                           col("source_file")
                       )
                      )

# COMMAND ----------

(df_comm_valid_final.write
 .mode("append")
 .option("mergeSchema","true")
 .saveAsTable(silver_comm)
)

# COMMAND ----------

df_comm_dq_bad_to_save = df_comm_dq_bad.select("*").withColumnRenamed("dq_reasons","dq_failure_reasons")

df_comm_ref_bad_to_save = (df_comm_ref_bad
                           .withColumn("dq_failure_reasons",
                                       F.coalesce(col("dq_reasons"), col("ref_failure")))
                           )

# COMMAND ----------

(df_comm_dq_bad_to_save.unionByName(df_comm_ref_bad_to_save, allowMissingColumns=True)
 .write.mode("append").option("mergeSchema","true")
 .saveAsTable(quarantine_comm)
)


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from zions_schema.silver_comm

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from zions_schema.quarantine_comm

# COMMAND ----------

