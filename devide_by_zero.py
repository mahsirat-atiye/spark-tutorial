import time
import datetime

from pyspark.shell import spark
from pyspark.sql import functions as f
import matplotlib.pyplot as plt
m = datetime.datetime.now()
df = spark.range(1).withColumn('time', f.lit(m))
df = df.withColumn('date', f.to_date(f.col('time')))
print(df.show())
df1 = spark.range(1).withColumn('time', f.lit(m))
df1 = df1.withColumn('date', f.to_date(f.col('time')))
print(df1.show())
print(df.join(df1, ["id", "time"]).show())
