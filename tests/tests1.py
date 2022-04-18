# Databricks notebook source
# MAGIC %run ../src/spark_map

# COMMAND ----------

from datetime import date
tb = spark.table('lima.job_bmg_eventtracks')\
  .filter(
    (F.col('Data') >= date(2022,3,1)) &
    (F.col('Data') <= date(2022,3,31))
  )

spark_across(tb, are_of_type('date'), F.add_months).display()
