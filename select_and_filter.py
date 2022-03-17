import time

from pyspark.shell import spark
from pyspark.sql import functions as f

# setup inputs
df = spark.range(1000).withColumn('id', f.col('id'))
df = df.withColumn('A', f.col('id'))
df = df.withColumn('B', f.col('id'))
df = df.withColumn('C', f.col('id'))
df = df.withColumn('E', f.col('id'))


# first senario :
begin = time.time()
result = df.select('id', 'A', 'B', 'C').filter(f.col('id') < 500)
result.show()
end = time.time()
print(end - begin)

# second senario
begin = time.time()

A = df.select('id', 'A').filter(f.col('id') < 500)
B = df.select('id', 'B').filter(f.col('id') < 500)
C = df.select('id', 'C').filter(f.col('id') < 500)

A_B_res = A.join(B, ['id'])
A_B_C_res = A_B_res.join(C, ['id'])
A_B_C_res.show()
end = time.time()

# Comment fist/ second senario and see the spark sql tab on localhost:4040
# Conclusion: spark is not smart enough to have equal palns for these to senarios
