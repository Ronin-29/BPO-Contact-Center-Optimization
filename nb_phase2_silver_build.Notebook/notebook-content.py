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
# META     }
# META   }
# META }

# CELL ********************

# BUILD A Clean labels, convert types, read both timestamp formats
from pyspark.sql import functions as F
from pyspark.sql.window import Window

bronze = spark.table("dbo.bronze_customer_tickets")

channel_clean = (
    F.when(F.upper(F.trim("Channel")) == "EMAIL", "Email")
     .when(F.upper(F.trim("Channel")).isin("VCE", "VOICE"), "Voice")
     .when(F.upper(F.trim("Channel")).isin("CHT", "CHAT"), "Chat")
     .otherwise(F.col("Channel"))
)
csat_clean = F.coalesce(
    F.expr("try_cast(CSAT_Rating AS INT)"),
    F.when(F.lower(F.trim("CSAT_Rating")) == "five", 5),
)
clean = bronze.select(
    F.col("Interaction_ID"),
    F.coalesce(
        F.expr("try_cast(Timestamp AS TIMESTAMP)"),
        F.expr("try_to_timestamp(Timestamp, 'dd/MM/yyyy HH:mm')"),
    ).alias("Timestamp"),
    F.col("Agent_ID"),
    channel_clean.alias("Channel"),
    F.initcap(F.trim("Queue_Name")).alias("Queue_Name"),
    F.expr("try_cast(Talk_Time_Sec AS INT)").alias("Talk_Time_Sec"),
    F.expr("try_cast(Hold_Time_Sec AS INT)").alias("Hold_Time_Sec"),
        csat_clean.alias("CSAT_Rating"),
    F.initcap(F.trim("Status")).alias("Status"),
    F.expr("try_cast(sim_date AS DATE)").alias("sim_date"),
    F.expr("try_cast(rand_pick AS DOUBLE)").alias("rand_pick"),
    F.col("CSAT_Rating").alias("raw_csat"),
    F.col("Talk_Time_Sec").alias("raw_talk"),
    F.col("Hold_Time_Sec").alias("raw_hold"),
)

clean.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# BUILD B Mark the problem rows
flagged = clean.withColumn(
    "quarantine_reason",
    F.concat_ws(
        "; ",
        F.when(F.col("raw_csat").isNotNull() & F.col("CSAT_Rating").isNull(),
               "CSAT not a number"),
        F.when(F.col("Agent_ID").isNull(), "Agent_ID missing"),
        F.when(F.col("Timestamp").isNull(), "Timestamp missing"),
        F.when(F.col("Talk_Time_Sec").isNull() | (F.col("Talk_Time_Sec") < 0),
               "Talk time invalid"),
        F.when(
            (F.col("raw_hold").isNotNull() & F.col("Hold_Time_Sec").isNull()
             & (F.upper(F.trim(F.col("raw_hold"))) != "N/A"))
            | (F.col("Hold_Time_Sec") < 0),
            "Hold time invalid"),
    )
)

flagged.groupBy("quarantine_reason").count().orderBy("count", ascending=False).show(30, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# BUILD C Split into good and quarantine
good = flagged.filter(F.col("quarantine_reason") == "").drop(
    "quarantine_reason", "raw_csat", "raw_talk", "raw_hold"
)
quarantine = flagged.filter(F.col("quarantine_reason") != "")

good_n = good.count()
quar_n = quarantine.count()

print("Good rows:      ", good_n)
print("Quarantine rows:", quar_n)
print("Total:          ", good_n + quar_n, "(should be 180000)")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# BUILD D Keep the latest row per ID, and check the date range
w = Window.partitionBy("Interaction_ID").orderBy(
    F.col("Timestamp").desc(),
    F.col("sim_date").desc(),
    F.col("rand_pick").desc(),
)

silver = (
    good.withColumn("rn", F.row_number().over(w))
        .filter(F.col("rn") == 1)
        .drop("rn")
)

print("Silver rows:", silver.count())
print("Distinct IDs in Silver:", silver.select("Interaction_ID").distinct().count())
print("Distinct IDs in Bronze:", bronze.select("Interaction_ID").distinct().count())

silver.agg(F.min("Timestamp"), F.max("Timestamp")).show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# BUILD E Which IDs are still lost?
lost_ids = (
    bronze.select("Interaction_ID").distinct()
          .join(silver.select("Interaction_ID"), "Interaction_ID", "left_anti")
)
print("IDs lost:", lost_ids.count())

lost_rows = quarantine.join(lost_ids, "Interaction_ID")
lost_rows.groupBy("quarantine_reason").count().orderBy("count", ascending=False).show(10, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# BUILD F: Write Silver
(silver.write
       .format("delta")
       .mode("overwrite")
       .option("overwriteSchema", "true")
       .saveAsTable("dbo.silver_customer_tickets"))

print("Silver written:", spark.table("dbo.silver_customer_tickets").count(), "rows")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# BUILD G: Write the quarantine table
(quarantine.write
           .format("delta")
           .mode("overwrite")
           .option("overwriteSchema", "true")
           .saveAsTable("dbo.quarantine_customer_tickets"))

print("Quarantine written:", spark.table("dbo.quarantine_customer_tickets").count(), "rows")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# BUILD H
silver_t = spark.table("dbo.silver_customer_tickets")
quar_t   = spark.table("dbo.quarantine_customer_tickets")

bronze_n     = bronze.count()
silver_n     = silver_t.count()
quar_n       = quar_t.count()
ids_bronze   = bronze.select("Interaction_ID").distinct().count()
ids_silver   = silver_t.select("Interaction_ID").distinct().count()

audit = spark.range(1).select(
    F.current_timestamp().alias("run_time"),
    F.lit(bronze_n).cast("long").alias("bronze_rows"),
    F.lit(silver_n).cast("long").alias("silver_rows"),
    F.lit(quar_n).cast("long").alias("quarantine_rows"),
    F.lit(ids_bronze).cast("long").alias("ids_in_bronze"),
    F.lit(ids_silver).cast("long").alias("ids_in_silver"),
    F.lit(ids_bronze - ids_silver).cast("long").alias("ids_lost"),
)

audit.write.format("delta").mode("append").saveAsTable("dbo.silver_dq_audit")
audit.show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# BUILD I Verify with row counts
silver_t = spark.table("dbo.silver_customer_tickets")
quar_t   = spark.table("dbo.quarantine_customer_tickets")

print("Silver rows:", silver_t.count())
print("Silver distinct IDs:", silver_t.select("Interaction_ID").distinct().count())
print("Quarantine rows:", quar_t.count())

print("Silver rows with null Agent_ID or Timestamp:",
      silver_t.filter(F.col("Agent_ID").isNull() | F.col("Timestamp").isNull()).count())
print("Silver rows with negative talk time:",
      silver_t.filter(F.col("Talk_Time_Sec") < 0).count())

silver_t.groupBy("CSAT_Rating").count().orderBy("CSAT_Rating").show()
quar_t.groupBy("quarantine_reason").count().orderBy("count", ascending=False).show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
