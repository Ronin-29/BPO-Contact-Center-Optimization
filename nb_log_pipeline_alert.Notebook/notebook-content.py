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

# PARAMETERS CELL ********************

# ALERT A  (toggle as parameter cell)
pipeline_name   = "pl_ingest_bpo_tickets"
run_id          = "manual-test"
failed_activity = "manual-test"
error_message   = "manual test of the alert logger"


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ALERT B Create the alerts table (if it doesn't exist)
spark.sql("CREATE SCHEMA IF NOT EXISTS control")

spark.sql("""
CREATE TABLE IF NOT EXISTS control.pipeline_alerts (
    alert_time       TIMESTAMP,
    pipeline_name    STRING,
    run_id           STRING,
    failed_activity  STRING,
    error_message    STRING
) USING DELTA
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ALERT C Write one alert row
from pyspark.sql import functions as F

alert_row = spark.range(1).select(
    F.current_timestamp().alias("alert_time"),
    F.lit(pipeline_name).alias("pipeline_name"),
    F.lit(run_id).alias("run_id"),
    F.lit(failed_activity).alias("failed_activity"),
    F.lit(error_message).alias("error_message"),
)

alert_row.write.format("delta").mode("append").saveAsTable("control.pipeline_alerts")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ALERT D Look at the alerts table
spark.table("control.pipeline_alerts").orderBy(F.col("alert_time").desc()).show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
