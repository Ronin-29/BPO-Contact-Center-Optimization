# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "3850ce3e-bf6e-4486-831f-ddb4cc861563",
# META       "default_lakehouse_name": "BPO_Optimization_Lakehouse",
# META       "default_lakehouse_workspace_id": "f8587821-d380-43d5-a84e-a6c435f190b9",
# META       "known_lakehouses": [
# META         {
# META           "id": "3850ce3e-bf6e-4486-831f-ddb4cc861563"
# META         }
# META       ]
# META     },
# META     "environment": {
# META       "environmentId": "8c2140ab-4cb8-9e37-4948-bc365113378c",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# CELL ********************

# Load the raw seed file uploaded in Phase 0, Step 1
df_seed = spark.read.option("header", True).csv("Files/raw_seed/bpo_messy_interactions.csv")
df_seed.show(5)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Fixed random seed so this dataset can be regenerated identically for debugging/testing
SEED = 42

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import pyspark.sql.functions as F

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_seed.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Safely fix the text numbers into real numbers
df_seed = df_seed.withColumn("Talk_Time_Sec", F.expr("try_cast(Talk_Time_Sec as int)"))
df_seed.filter(F.col("Talk_Time_Sec").isNull()).count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

# Generate a 3-year daily date spine, then cross-join against seed rows to simulate 3 years of tickets
date_spine = spark.sql("""
    SELECT explode(sequence(to_date('2024-01-01'), to_date('2026-12-31'), interval 1 day)) as sim_date
""")

df_expanded = df_seed.crossJoin(date_spine) \
    .withColumn("rand_pick", F.rand(SEED)) \
    .orderBy("rand_pick") \
    .limit(180000)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

   df_expanded.show(5)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

   df_expanded.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

   df_expanded.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

   df_expanded.select(F.min("sim_date"), F.max("sim_date")).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Calculate mean/stddev from the original 5,000-row sample, then generate realistic values around it
stats = df_seed.select(F.mean("Talk_Time_Sec").alias("mu"), F.stddev("Talk_Time_Sec").alias("sigma")).first()

df_expanded = df_expanded.withColumn(
    "Talk_Time_Sec", (F.randn(SEED) * stats["sigma"] + stats["mu"]).cast("int")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Re-inject controlled anomalies so Silver-layer cleaning has real messiness to fix
df_expanded = df_expanded.withColumn(
    "CSAT_Rating",
    F.when(F.rand(SEED) < 0.02, "Five")  # ~2% of rows get CSAT written as text
     .otherwise(F.col("CSAT_Rating"))
)

df_expanded = df_expanded.withColumn(
    "Agent_ID",
    F.when(F.rand(SEED) < 0.01, None)  # ~1% of rows get a missing Agent_ID
     .otherwise(F.col("Agent_ID"))
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("CSAT as text count:", df_expanded.filter(F.col("CSAT_Rating") == "Five").count())
print("Null Agent_ID count:", df_expanded.filter(F.col("Agent_ID").isNull()).count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_expanded.write.mode("overwrite").option("header", True).csv("Files/landing/bronze_customer_tickets_raw/")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(notebookutils.fs.ls("Files/landing/bronze_customer_tickets_raw/"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_check = spark.read.option("header", True).csv("Files/landing/bronze_customer_tickets_raw/")
print("Rows written:", df_check.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Phase 0: original synthetic data" ends and "New batch: incremental load demo**

# CELL ********************

from pyspark.sql import functions as F

SEED = 99  # different from Phase 0's seed, since this is unrelated new data

df_batch = spark.range(0, 2000).select(
    F.concat(F.lit("TICK-B"), F.col("id").cast("string")).alias("Interaction_ID"),
    F.date_format(
        F.expr("date_add('2027-01-01', cast(rand({}) * 30 as int))".format(SEED)),
        "yyyy-MM-dd HH:mm:ss"
    ).alias("Timestamp"),
    F.concat(F.lit("Agent_"), (F.rand(SEED) * 50).cast("int").cast("string")).alias("Agent_ID"),
    F.array(F.lit("Voice"), F.lit("Chat"), F.lit("Email"))[(F.rand(SEED) * 3).cast("int")].alias("Channel"),
    F.array(F.lit("Billing"), F.lit("Tech Support"), F.lit("General"))[(F.rand(SEED) * 3).cast("int")].alias("Queue_Name"),
    (F.rand(SEED) * 1000 + 60).cast("int").cast("string").alias("Talk_Time_Sec"),
    (F.rand(SEED) * 200).cast("int").cast("string").alias("Hold_Time_Sec"),
    ((F.rand(SEED) * 5).cast("int") + 1).cast("string").alias("CSAT_Rating"),
    F.array(F.lit("Resolved"), F.lit("Open"), F.lit("Dropped"))[(F.rand(SEED) * 3).cast("int")].alias("Status"),
    F.date_format(
        F.expr("date_add('2027-01-01', cast(rand({}) * 30 as int))".format(SEED)),
        "yyyy-MM-dd"
    ).alias("sim_date"),
    F.rand(SEED).cast("string").alias("rand_pick")
)

df_batch.coalesce(1).write.mode("overwrite").option("header", True).csv("Files/landing/new_batch_raw/")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(spark.read.option("header", True).csv("Files/landing/new_batch_raw/"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(spark.read.option("header", True).csv("Files/landing/new_batch_raw/").count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

notebookutils.fs.rm("Files/landing/new_batch_raw/", True)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(notebookutils.fs.ls("Files/landing/new_batch_raw/"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT COUNT(*) AS staging_rows FROM dbo.stg_customer_tickets;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT COUNT(*) AS bronze_rows FROM dbo.bronze_customer_tickets;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT * FROM control.load_log;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql 
# MAGIC SELECT
# MAGIC   CASE WHEN Interaction_ID LIKE 'TICK-B%' THEN 'new_batch' ELSE 'original' END AS source_type,
# MAGIC   COUNT(*) AS row_count
# MAGIC FROM dbo.bronze_customer_tickets
# MAGIC GROUP BY CASE WHEN Interaction_ID LIKE 'TICK-B%' THEN 'new_batch' ELSE 'original' END;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT Interaction_ID, COUNT(*) AS times_seen
# MAGIC FROM dbo.bronze_customer_tickets
# MAGIC WHERE Interaction_ID NOT LIKE 'TICK-B%'
# MAGIC GROUP BY Interaction_ID
# MAGIC HAVING COUNT(*) > 1
# MAGIC ORDER BY times_seen DESC;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

