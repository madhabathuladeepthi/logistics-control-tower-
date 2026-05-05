from pyspark.sql import SparkSession
from pyspark.sql.functions import unix_timestamp

spark = SparkSession.builder.appName("LogiScale").getOrCreate()

df = spark.read.csv(
    "C:/Users/vikhy/OneDrive/Documents/Deepthi/INFOTACT/LogiScalebigData/shipments.csv",
    header=True,
    inferSchema=True
)

df = df.withColumn(
    "delay_minutes",
    (unix_timestamp("actual_delivery") - unix_timestamp("eta")) / 60
)

from pyspark.sql.functions import avg, max, min

agg_df = df.groupBy("route_id").agg(
    avg("delay_minutes").alias("avg_delay"),
    max("delay_minutes").alias("max_delay"),
    min("delay_minutes").alias("min_delay")
)
agg_df.show()

agg_df.write.mode("overwrite").parquet("output/shipments_parquet")
test_df = spark.read.parquet("output/shipments_parquet")
test_df.show()

from pyspark.sql.window import Window
from pyspark.sql.functions import avg

window_spec = Window.partitionBy("route_id").orderBy("eta")

df = df.withColumn(
    "running_avg_delay",
    avg("delay_minutes").over(window_spec)
)

df.show()

from pyspark.sql.window import Window
from pyspark.sql.functions import avg

window_spec_rolling = Window.partitionBy("route_id") \
    .orderBy("eta") \
    .rowsBetween(-1, 0)   # last 2 rows

df = df.withColumn(
    "rolling_avg_last2",
    avg("delay_minutes").over(window_spec_rolling)
)

df.show()

df.groupBy("route_id") \
  .avg("delay_minutes") \
  .orderBy("avg(delay_minutes)", ascending=False) \
  .limit(5) \
  .show()
df.select("route_id", "delay_minutes").show()

df.explain(True)
