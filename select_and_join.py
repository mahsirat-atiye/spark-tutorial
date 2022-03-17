import time

from pyspark.shell import spark
from pyspark.sql import functions as f

# setup configs
# https://stackoverflow.com/questions/62489559/order-of-table-joining-in-sparksql-for-better-performance/62495763#62495763
spark.conf.set("spark.sql.cbo.enabled", True)
spark.conf.set("spark.sql.cbo.joinReorder.enabled", True)
spark.conf.set("spark.sql.statistics.histogram.enabled", True)

# setup inputs
df = spark.range(1000).withColumn('id', f.col('id'))
df = df.withColumn('A', f.col('id'))
df = df.withColumn('B', f.col('id'))
df = df.withColumn('C', f.col('id'))
df = df.withColumn('E', f.col('id'))

cohort = spark.range(500).withColumn('id', f.col('id'))

# first senario :
begin = time.time()
result = df.select('id', 'A', 'B', 'C').join(cohort, ['id'])
result.show()
end = time.time()
print(end - begin)

# second senario
begin = time.time()

A = df.select('id', 'A').join(cohort, ['id'])
B = df.select('id', 'B').join(cohort, ['id'])
C = df.select('id', 'C').join(cohort, ['id'])

A_B_res = A.join(B, ['id'])
A_B_C_res = A_B_res.join(C, ['id'])
A_B_C_res.show()
end = time.time()

# Comment fist/ second senario and see the spark sql tab on localhost:4040
# Conclusion: spark is not smart enough to have equal palns for these to senarios
