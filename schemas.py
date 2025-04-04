from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

title_ratings_schema = StructType([
        StructField("tconst", StringType(), True),
        StructField("averageRating", DoubleType(), True),
        StructField("numVotes", IntegerType(), True)
    ])


title_episode_schema = StructType([
        StructField("tconst", StringType(), True),
        StructField("parentTconst", StringType(), True),
        StructField("seasonNumber", StringType(), True),
        StructField("episodeNumber", StringType(), True)
    ])