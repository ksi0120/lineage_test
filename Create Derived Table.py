# Databricks notebook source
# DBTITLE 1,Read source table, apply perturbation, write derived table
from pyspark.sql import functions as F

CATALOG = "classic_stable_3fqn9z_catalog"
SCHEMA = "lineage_test"

# Read from source table
source = spark.table(f"{CATALOG}.{SCHEMA}.lineage_source1")

# Apply perturbation: add gaussian noise, derive new columns, randomly drop ~5% of rows
perturbed = (
    source
    .withColumn("value_a_perturbed", F.round(F.col("value_a") + F.randn(seed=21) * 5, 2))
    .withColumn("value_b_perturbed", F.round(F.col("value_b") * (1 + F.randn(seed=33) * 0.1), 2))
    .withColumn("value_ratio", F.round(
        F.col("value_a") / F.when(F.col("value_b") == 0, F.lit(1)).otherwise(F.col("value_b")),
        4
    ))
    .withColumn("is_outlier", F.when(F.abs(F.col("value_a") - 50) > 30, True).otherwise(False))
    .filter(F.rand(seed=55) > 0.05)  # Randomly drop ~5% of rows
)

perturbed.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.lineage_derived")
display(spark.table(f"{CATALOG}.{SCHEMA}.lineage_derived"))
