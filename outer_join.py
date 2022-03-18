from pyspark.shell import spark
from pyspark.sql import functions as f

A = spark.range(100, 1000).withColumn('id', f.col('id')).withColumn('A', f.col('id')).withColumn('is_odd', f.when(
    f.col('id') % 2 == 1, 0).otherwise(1))

B = spark.range(200).withColumn('id', f.col('id')).withColumn('B', f.col('id')).withColumn('is_odd', f.when(
    f.col('id') % 2 == 1, 0).otherwise(1))

A_B_outer = A.join(B, ['id'], 'outer')
print(A_B_outer.count())
# print(A_B_outer.show())
