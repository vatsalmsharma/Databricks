# Databricks notebook source
silver_cust_table = "zions_schema.silver_cust"
dim_table = "zions_schema.dim_customer_scd2" 

# COMMAND ----------

df_events = spark.table(silver_cust_table)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from zions_schema.silver_cust

# COMMAND ----------

spark.sql(f"""CREATE OR REPLACE TEMP VIEW vw_events_dedup AS
SELECT
  cust_id,
  event_type,
  event_ts_parsed,            
  first_name,
  last_name,
  email,
  phone,
  addr_city,
  addr_state,
  addr_postal,
  ingest_ts,
  source_file,
  ROW_NUMBER() OVER (
    PARTITION BY cust_id, event_ts_parsed, first_name, last_name, email, phone, addr_city, addr_state, addr_postal
    ORDER BY ingest_ts DESC NULLS LAST
  ) AS rn
FROM {silver_cust_table}
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW vw_events_for_scd AS
# MAGIC SELECT *
# MAGIC FROM vw_events_dedup
# MAGIC WHERE rn = 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from vw_events_dedup 
# MAGIC ORDER BY cust_id, event_ts_parsed, ingest_ts;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW vw_dim_reconstructed AS
# MAGIC SELECT
# MAGIC   cust_id,
# MAGIC   first_name,
# MAGIC   last_name,
# MAGIC   email,
# MAGIC   phone,
# MAGIC   addr_city,
# MAGIC   addr_state,
# MAGIC   addr_postal,
# MAGIC   event_ts_parsed AS eff_start,
# MAGIC   LEAD(event_ts_parsed) OVER (PARTITION BY cust_id ORDER BY event_ts_parsed, ingest_ts) AS eff_end,
# MAGIC   CASE 
# MAGIC     WHEN LEAD(event_ts_parsed) OVER (PARTITION BY cust_id ORDER BY event_ts_parsed, ingest_ts) IS NULL 
# MAGIC       THEN true 
# MAGIC     ELSE false 
# MAGIC   END AS is_current,
# MAGIC   event_type,
# MAGIC   ingest_ts,
# MAGIC   source_file
# MAGIC
# MAGIC FROM vw_events_for_scd
# MAGIC ORDER BY cust_id, eff_start;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from vw_dim_reconstructed

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW vw_dim_final AS
# MAGIC SELECT
# MAGIC  row_number() OVER (ORDER BY cust_id, eff_start) AS scd_row_id,
# MAGIC   cust_id,
# MAGIC   first_name,
# MAGIC   last_name,
# MAGIC   email,
# MAGIC   phone,
# MAGIC   addr_city,
# MAGIC   addr_state,
# MAGIC   addr_postal,
# MAGIC   eff_start,
# MAGIC   eff_end,
# MAGIC   is_current,
# MAGIC   event_type,
# MAGIC   ingest_ts,
# MAGIC   source_file
# MAGIC FROM vw_dim_reconstructed;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM vw_dim_final

# COMMAND ----------

dim_df = spark.sql("SELECT * FROM vw_dim_final")


(dim_df.write
  .mode("overwrite")
  .option("overwriteSchema", "true")
  .saveAsTable(dim_table))

# COMMAND ----------

spark.sql(f"SELECT * FROM {dim_table} ORDER BY cust_id, eff_start").show(20, False)

# COMMAND ----------

