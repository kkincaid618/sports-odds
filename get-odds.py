# Databricks notebook source
from get_data import PullData
from pyspark.sql import SparkSession

# COMMAND ----------

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

api_key = '76596681042e0510c8311d07f25ca69c'
puller = PullData(api_key)
df = puller.grab_data()

# COMMAND ----------

spark_df = spark.createDataFrame(df)
spark_df.write.mode("append").saveAsTable("ncaa_odds")
