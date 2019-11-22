from pyspark.sql import SparkSession

def createSparkSession():
    return SparkSession.builder.appName("pyspark_evv_cust").master("local[*]").getOrCreate()