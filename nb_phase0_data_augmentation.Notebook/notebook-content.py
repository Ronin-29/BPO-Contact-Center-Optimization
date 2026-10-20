# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "53aca8b1-e3ae-40f8-bc21-ecf8d849a5db",
# META       "default_lakehouse_name": "BPO_Optimization_Lakehouse",
# META       "default_lakehouse_workspace_id": "2a14efbb-e4ad-40b0-ac12-4b64b4ebfec1",
# META       "known_lakehouses": [
# META         {
# META           "id": "53aca8b1-e3ae-40f8-bc21-ecf8d849a5db"
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

# MARKDOWN ********************

# Cell 5A: Build the rows, IDs and Timestamp

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql import Window

# 1. Date spine: one row per day, 2024-01-01 to 2026-12-31 (same range as before)
date_spine = spark.sql("""
    SELECT explode(sequence(to_date('2024-01-01'), to_date('2026-12-31'), interval 1 day)) AS sim_date
""")

# 2. Cross join seed rows with the dates, shuffle randomly, keep 180,000
#    We drop the seed's own Interaction_ID and Timestamp because we rebuild both below
df_expanded = (
    df_seed.drop("Interaction_ID", "Timestamp")
    .crossJoin(date_spine)
    .withColumn("rand_pick", F.rand(SEED))
    .orderBy("rand_pick")
    .limit(180000)
)

# 3. Give every row its own unique Interaction_ID (TICK-000001 ... TICK-180000)
w = Window.orderBy("sim_date", "rand_pick")
df_expanded = df_expanded.withColumn("row_num", F.row_number().over(w))
df_expanded = df_expanded.withColumn(
    "Interaction_ID",
    F.concat(F.lit("TICK-"), F.lpad(F.col("row_num").cast("string"), 6, "0"))
)

# 4. Build Timestamp = the row's date + a random time of day
df_expanded = df_expanded.withColumn("seconds_into_day", (F.rand(SEED + 1) * 86400).cast("int"))
df_expanded = df_expanded.withColumn(
    "Timestamp",
    F.from_unixtime(
        F.unix_timestamp(F.col("sim_date").cast("timestamp")) + F.col("seconds_into_day"),
        "yyyy-MM-dd HH:mm:ss"
    )
)

# 5. Keep the original Bronze column order and drop the helper columns
df_expanded = df_expanded.select(
    "Interaction_ID", "Timestamp", "Agent_ID", "Channel", "Queue_Name",
    "Talk_Time_Sec", "Hold_Time_Sec", "CSAT_Rating", "Status", "sim_date", "rand_pick"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Cell 5B: Check the result

# CELL ********************

print("Rows:", df_expanded.count())
print("Distinct IDs:", df_expanded.select("Interaction_ID").distinct().count())
print("Timestamp date different from sim_date:",
      df_expanded.filter(F.to_date("Timestamp") != F.col("sim_date")).count())
df_expanded.select(F.min("Timestamp"), F.max("Timestamp")).show(truncate=False)
df_expanded.show(5, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Mean and spread of Talk_Time_Sec from the 5,000-row seed (nulls are ignored)
stats = df_seed.select(F.mean("Talk_Time_Sec").alias("mu"), F.stddev("Talk_Time_Sec").alias("sigma")).first()

# Replace Talk_Time_Sec with random values around that mean (seed 43)
df_expanded = df_expanded.withColumn(
    "Talk_Time_Sec", (F.randn(43) * stats["sigma"] + stats["mu"]).cast("int")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ~2% of rows get CSAT written as text (seed 44)
df_expanded = df_expanded.withColumn(
    "CSAT_Rating",
    F.when(F.rand(44) < 0.02, "Five").otherwise(F.col("CSAT_Rating"))
)

# ~1% of rows get a missing Agent_ID (seed 45, different from the CSAT seed)
df_expanded = df_expanded.withColumn(
    "Agent_ID",
    F.when(F.rand(45) < 0.01, None).otherwise(F.col("Agent_ID"))
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

# MARKDOWN ********************

# Cell 9B: Inject messy dates (new cell, run after Cell 9)

# CELL ********************

# ~3% of rows get Timestamp written as dd/MM/yyyy HH:mm (seed 46)
df_expanded = df_expanded.withColumn(
    "Timestamp",
    F.when(
        F.rand(46) < 0.03,
        F.date_format(F.to_timestamp("Timestamp"), "dd/MM/yyyy HH:mm")
    ).otherwise(F.col("Timestamp"))
)

# Check: how many rows have each format
pattern_messy = r"^\d{2}/\d{2}/\d{4}"
print("Rows with dd/MM/yyyy format:", df_expanded.filter(F.col("Timestamp").rlike(pattern_messy)).count())
print("Rows with normal format:", df_expanded.filter(~F.col("Timestamp").rlike(pattern_messy)).count())
df_expanded.filter(F.col("Timestamp").rlike(pattern_messy)) \
    .select("Interaction_ID", "Timestamp", "sim_date").show(5, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Write ONE csv file (coalesce(1) joins all pieces into a single part file)
df_expanded.coalesce(1).write.mode("overwrite").option("header", True).csv("Files/landing/bronze_customer_tickets_raw/")

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

# MARKDOWN ********************


# CELL ********************

# List what is actually inside the Files area of the attached lakehouse
display(notebookutils.fs.ls("Files/"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Show the tables in the lakehouse this notebook is attached to
spark.sql("SHOW TABLES").show(50, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
