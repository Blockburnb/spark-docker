from pyspark.sql import SparkSession

spark = SparkSession.builder.master("spark://spark-master:7077").appName("GiftCardsSparkTest").getOrCreate()
print('Spark version:', spark.version)

df = spark.read.json('/home/jovyan/work/meta_Gift_Cards.jsonl')
print('Schema:')
df.printSchema()
print('Count:', df.count())
print('Show 5 rows:')
df.show(5)

spark.stop()
