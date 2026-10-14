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
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql import functions as F

bronze = spark.table("dbo.bronze_customer_tickets")

bronze.printSchema()
bronze.show(5, truncate=False)

print("Total rows:", bronze.count())
print("Distinct Interaction_ID:", bronze.select("Interaction_ID").distinct().count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

copies = bronze.groupBy("Interaction_ID").count().withColumnRenamed("count", "copies")

copies.groupBy("copies").agg(F.count("*").alias("num_ids")).orderBy("copies").show(50)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

skip_cols = []   # add any load-time columns from Cell 1, e.g. ["load_timestamp"]

compare = bronze.drop(*skip_cols)

print("Distinct full rows:", compare.distinct().count())
print("Distinct Interaction_ID:", bronze.select("Interaction_ID").distinct().count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

null_counts = bronze.select(
    [F.count(F.when(F.col(c).isNull(), 1)).alias(c) for c in bronze.columns]
)
null_counts.show(vertical=True)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

bronze.groupBy("CSAT_Rating").count().orderBy("CSAT_Rating").show(50, truncate=False)

bad_csat = bronze.filter(
    F.col("CSAT_Rating").isNotNull() &
    F.expr("try_cast(CSAT_Rating AS INT)").isNull()
)
print("CSAT values that are not numbers:", bad_csat.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

bad_csat_cond = F.col("CSAT_Rating").isNotNull() & F.expr("try_cast(CSAT_Rating AS INT)").isNull()
null_agent_cond = F.col("Agent_ID").isNull()

print("Bad CSAT:", bronze.filter(bad_csat_cond).count())
print("Null Agent_ID:", bronze.filter(null_agent_cond).count())
print("Both:", bronze.filter(bad_csat_cond & null_agent_cond).count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for c in ["Channel", "Queue_Name", "Status"]:
    print("Column:", c)
    bronze.groupBy(c).count().orderBy("count", ascending=False).show(50, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

bronze.select(
    F.min(F.expr("try_cast(Timestamp AS TIMESTAMP)")).alias("min_ts"),
    F.max(F.expr("try_cast(Timestamp AS TIMESTAMP)")).alias("max_ts"),
    F.min(F.expr("try_cast(sim_date AS DATE)")).alias("min_sim"),
    F.max(F.expr("try_cast(sim_date AS DATE)")).alias("max_sim"),
).show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
