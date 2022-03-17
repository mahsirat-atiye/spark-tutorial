import time

from pyspark.shell import spark
from pyspark.sql import functions as f
import matplotlib.pyplot as plt



x = [i for i in range(1, 10)]
ts =[]
for n in x:
    df = spark.range(1000).withColumn('id', f.col('id'))
    s = time.time()
    aggs = []
    for j in range(n):
        aggs.append(f.sum(f.col('id')).alias(f'sum_{j}'))

    grouped = df.groupBy(f.col('id'))
    res = grouped.agg(*aggs)
    e = time.time()
    ts.append((e-s))

plt.plot(x, ts)
plt.show()



