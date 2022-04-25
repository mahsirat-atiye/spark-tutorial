import time

from pyspark.shell import spark
from pyspark.sql import functions as f
import matplotlib.pyplot as plt
from pyspark.sql.window import Window

x = [i for i in range(1, 50)]
ts =[]
for n in x:
    df = spark.range(1000).withColumn('id', f.col('id'))
    s = time.time()
    for j in range(n):
        df = df.withColumn(f'sum_{j}', f.sum('id').over(Window.partitionBy('id')))
    e = time.time()
    ts.append((e-s))

plt.plot(x, ts)
plt.show()



