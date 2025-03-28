from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ReadCSV").getOrCreate()

df = spark.read.option("header", "true").option("sep", "\t").csv("imdb_dataset/title.episode.tsv")
df.show()

df_title_basics = spark.read.option("header", "true").option("sep", "\t").csv("imdb_dataset/title.basics.tsv")
df_title_basics.printSchema()
df_title_basics.show(10)
df_title_ratings = spark.read.option("header", "true").option("sep", "\t").csv("imdb_dataset/title.ratings.tsv")
df_title_ratings.printSchema()
df_title_ratings.show(10)

df_name_basics = spark.read.option("header", "true").option("sep", "\t").csv("imbd_dataset/name.basics.tsv")
df_name_basics.printSchema()
df_name_basics.show(5)
df_title_akas = spark.read.option("header", "true").option("sep", "\t").csv("imbd_dataset/title.akas.tsv")
df_title_akas.printSchema()
df_title_akas.show(5)

df_title_ratings = spark.read.option("header", "true").option("sep", "\t").csv("imdb_dataset/title.crew.tsv")
df_title_ratings.printSchema()
df_title_ratings.show(5)
df_title_ratings = spark.read.option("header", "true").option("sep", "\t").csv("imdb_dataset/title.principals.tsv")
df_title_ratings.printSchema()
df_title_ratings.show(5)