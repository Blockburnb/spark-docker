import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg

start_total = time.time()
spark = SparkSession.builder.master("spark://spark-master:7077").appName('GiftCardsOps').getOrCreate()
print('Spark version:', spark.version)

start = time.time()
df = spark.read.json('/home/jovyan/work/meta_Gift_Cards.jsonl')
read_time = time.time() - start
print('spark_read_time:', read_time)

count = df.count()
print('count:', count)

print('select title, average_rating (10 rows):')
df.select('title','average_rating').show(10, truncate=80)

print('filter average_rating > 4.5 (10 rows):')
df.filter(df.average_rating > 4.5).show(10, truncate=80)

print('average rating (spark):')
df.select(avg('average_rating')).show()

# partitions
rdd = df.rdd
print('num_partitions_before:', rdd.getNumPartitions())

repart = df.repartition(8)
print('num_partitions_after_repartition_call (logical):', repart.rdd.getNumPartitions())

end_total = time.time()
print('total_job_time:', end_total - start_total)

spark.stop()
