#from spark_map.functions import spark_map
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()


students = spark.read.parquet("data/students.parquet")
transf = spark.read.parquet("data/transf.parquet")
transf.printSchema()