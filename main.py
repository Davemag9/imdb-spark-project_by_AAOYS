from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ReadCSV").getOrCreate()

df = spark.read.option("header", "true").option("sep", "\t").csv("imdb_dataset/title.episode.tsv")
df.show()
