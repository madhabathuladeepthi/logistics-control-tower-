from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Test").getOrCreate()

data = [("A", 10), ("B", 20)]
df = spark.createDataFrame(data, ["Name", "Value"])

df.show()