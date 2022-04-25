import time

from pyspark.shell import spark
from pyspark.sql import functions as f
import matplotlib.pyplot as plt

# dfs = [spark.range(1000).withColumn('id', f.col('id')) for i in range(1, 150)]
# x = [i for i in range(1, 150)]
# ts = []
#
#
# for n in x:
#     df = spark.range(1000).withColumn('id', f.col('id'))
#     s = time.time()
#     for i in range(n):
#         df = df.join(dfs[i], 'id')
#     e = time.time()
#     ts.append((e - s))
#
# plt.plot(x, ts)
# plt.show()

x = [i for i in range(1, 150)]
ts = []


for n in x:
    df = spark.range(10000*n).withColumn('id', f.col('id'))
    dfs = [spark.range(1000*n).withColumn('id', f.col('id')) for i in range(1, 15)]
    s = time.time()
    for i in range(14):
        df = df.join(dfs[i], 'id')
    e = time.time()
    ts.append((e - s))

plt.plot(x, ts)
plt.show()