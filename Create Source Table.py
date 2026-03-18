# Databricks notebook source
# DBTITLE 1,Create source table with 500 random rows
from pyspark.sql import functions as F

CATALOG = "classic_stable_3fqn9z_catalog"
SCHEMA = "lineage_test"

# Create source table with 500 random values
df = (
    spark.range(500)
    .select(
        F.col("id"),
        F.round(F.rand(seed=42) * 100, 2).alias("value_a"),
        F.round(F.rand(seed=7) * 50, 2).alias("value_b"),
        F.array(
            F.lit("alpha"), F.lit("beta"), F.lit("gamma"), F.lit("delta")
        ).getItem((F.rand(seed=13) * 4).cast("int")).alias("category"),
        F.date_add(F.lit("2025-01-01"), (F.rand(seed=99) * 365).cast("int")).alias("event_date"),
    )
)

df.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.lineage_source")
display(spark.table(f"{CATALOG}.{SCHEMA}.lineage_source"))