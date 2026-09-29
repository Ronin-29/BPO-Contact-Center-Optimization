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

spark.sql("CREATE SCHEMA IF NOT EXISTS control")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""
CREATE TABLE IF NOT EXISTS control.load_log (
    load_id          STRING,
    source_table     STRING,
    last_loaded_date DATE,
    rows_loaded      BIGINT,
    status           STRING,
    environment      STRING,
    loaded_at        TIMESTAMP
) USING DELTA
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if spark.table("control.load_log").count() == 0:
    spark.sql("""
    INSERT INTO control.load_log
    VALUES ('seed', 'bronze_customer_tickets', DATE'1900-01-01', 0,
            'initial', 'dev', current_timestamp())
    """)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(spark.table("control.load_log"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
