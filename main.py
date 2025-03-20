from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ReadCSV").getOrCreate()

df = spark.read.option("header", "true").option("sep", "\t").csv("imdb_dataset/title.episode.tsv")
df.show()

df_title_basics = spark.read.option("header", "true").option("sep", "\t").csv("imdb_dataset/title.basics.tsv")
df_title_basics.printSchema()
df_title_basics.show(5)
df_title_ratings = spark.read.option("header", "true").option("sep", "\t").csv("imdb_dataset/title.ratings.tsv")
df_title_ratings.printSchema()
df_title_ratings.show(5)