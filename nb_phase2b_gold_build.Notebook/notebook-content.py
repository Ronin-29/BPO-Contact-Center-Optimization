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

# Cell 1: Look at Silver before building 

silver = spark.table("dbo.silver_customer_tickets")
silver.printSchema()
print("Silver rows:", silver.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 2: Build dim_agent

from pyspark.sql import functions as F
from pyspark.sql.window import Window

silver = spark.table("dbo.silver_customer_tickets")

dim_agent = (
    silver.select("Agent_ID").distinct()
    .withColumn("agent_key", F.row_number().over(Window.orderBy("Agent_ID")))
    .select("agent_key", "Agent_ID")
)

(dim_agent.write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true").saveAsTable("dbo.dim_agent"))

print("dim_agent rows:", dim_agent.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 3: Build dim_channel and dim_queue

dim_channel = (
    silver.select(F.coalesce("Channel", F.lit("Unknown")).alias("Channel")).distinct()
    .withColumn("channel_key", F.row_number().over(Window.orderBy("Channel")))
    .select("channel_key", "Channel")
)

dim_queue = (
    silver.select(F.coalesce("Queue_Name", F.lit("Unknown")).alias("Queue_Name")).distinct()
    .withColumn("queue_key", F.row_number().over(Window.orderBy("Queue_Name")))
    .select("queue_key", "Queue_Name")
)

(dim_channel.write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true").saveAsTable("dbo.dim_channel"))
(dim_queue.write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true").saveAsTable("dbo.dim_queue"))

dim_channel.show()
dim_queue.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 4: Build dim_date

dim_date = (
    spark.sql("""
        SELECT explode(sequence(to_date('2024-01-01'), to_date('2027-12-31'), interval 1 day)) AS full_date
    """)
    .select(
        (F.year("full_date") * 10000 + F.month("full_date") * 100 + F.dayofmonth("full_date")).alias("date_key"),
        "full_date",
        F.year("full_date").alias("year"),
        F.quarter("full_date").alias("quarter"),
        F.month("full_date").alias("month_number"),
        F.date_format("full_date", "MMMM").alias("month_name"),
        F.dayofmonth("full_date").alias("day_of_month"),
        F.dayofweek("full_date").alias("day_of_week_number"),   # 1 = Sunday
        F.date_format("full_date", "EEEE").alias("day_name"),
        F.dayofweek("full_date").isin(1, 7).alias("is_weekend"),
    )
)

(dim_date.write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true").saveAsTable("dbo.dim_date"))

print("dim_date rows:", dim_date.count())   # expect 1,461

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 5: Build fact_interactions

d_agent   = spark.table("dbo.dim_agent")
d_channel = spark.table("dbo.dim_channel")
d_queue   = spark.table("dbo.dim_queue")
d_date    = spark.table("dbo.dim_date").select("date_key", "full_date")

fact_interactions = (
    silver
    .withColumn("Channel", F.coalesce("Channel", F.lit("Unknown")))
    .withColumn("Queue_Name", F.coalesce("Queue_Name", F.lit("Unknown")))
    .withColumn("full_date", F.to_date("Timestamp"))
    .join(d_agent,   "Agent_ID",   "inner")
    .join(d_channel, "Channel",    "inner")
    .join(d_queue,   "Queue_Name", "inner")
    .join(d_date,    "full_date",  "inner")
    .select(
        "Interaction_ID",
        "date_key", "agent_key", "channel_key", "queue_key",
        F.col("Timestamp").alias("interaction_timestamp"),
        "Status",
        "Talk_Time_Sec",
        "Hold_Time_Sec",
        (F.col("Talk_Time_Sec") + F.coalesce(F.col("Hold_Time_Sec"), F.lit(0))).alias("handle_time_sec"),
        "CSAT_Rating",
    )
)

(fact_interactions.write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true").saveAsTable("dbo.fact_interactions"))

print("fact rows:", fact_interactions.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 6: Verify

fact = spark.table("dbo.fact_interactions")

print("Silver rows:        ", silver.count())
print("Fact rows:          ", fact.count())
print("Distinct IDs in fact:", fact.select("Interaction_ID").distinct().count())

for name, dim, key in [("agent", "dbo.dim_agent", "agent_key"),
                       ("channel", "dbo.dim_channel", "channel_key"),
                       ("queue", "dbo.dim_queue", "queue_key"),
                       ("date", "dbo.dim_date", "date_key")]:
    orphans = fact.join(spark.table(dim), key, "left_anti").count()
    print(f"Orphan {name} keys:", orphans)

print("Unknown channel rows:", fact.join(spark.table("dbo.dim_channel").filter("Channel = 'Unknown'"), "channel_key").count())
print("Unknown queue rows:  ", fact.join(spark.table("dbo.dim_queue").filter("Queue_Name = 'Unknown'"), "queue_key").count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
