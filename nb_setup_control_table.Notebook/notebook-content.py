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

def update_watermark(new_last_loaded_date, rows_loaded, status, environment, load_id):
    spark.sql(f"""
    MERGE INTO control.load_log AS target
    USING (SELECT '{load_id}' AS load_id,
                  'bronze_customer_tickets' AS source_table,
                  DATE'{new_last_loaded_date}' AS last_loaded_date,
                  {rows_loaded} AS rows_loaded,
                  '{status}' AS status,
                  '{environment}' AS environment,
                  current_timestamp() AS loaded_at) AS source
    ON target.source_table = source.source_table
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    """)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

# Parameters cell — values will be overridden by the pipeline at run time
run_load_date = "1900-01-01"
run_environment = "dev"
run_id = "manual-test"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

newest_date_row = spark.sql("""
    SELECT MAX(try_cast(sim_date AS DATE)) AS newest_date
    FROM dbo.stg_customer_tickets
""").collect()[0]

rows_loaded_count = spark.sql("""
    SELECT COUNT(*) AS cnt
    FROM dbo.stg_customer_tickets
    WHERE try_cast(sim_date AS DATE) > DATE('{}')
""".format(run_load_date)).collect()[0]["cnt"]

update_watermark(
    str(newest_date_row["newest_date"]),
    rows_loaded_count,
    "success",
    run_environment,
    run_id
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

run_load_date = "1900-01-01"
run_environment = "dev"
run_id = "manual-test"

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
