from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, BooleanType

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

title_akas_schema = StructType([
    StructField("titleId", StringType(), True),
    StructField("ordering", IntegerType(), True),
    StructField("title", StringType(), True),
    StructField("region", StringType(), True),
    StructField("language", StringType(), True),
    StructField("types", StringType(), True),       # буде строка з комами → можна потім розбити
    StructField("attributes", StringType(), True),  # те ж саме
    StructField("isOriginalTitle", StringType(), True)
])

title_crew_schema = StructType([
    StructField("tconst", StringType(), True),
    StructField("directors", StringType(), True),
    StructField("writers", StringType(), True)
])


title_basics_schema = StructType([
    StructField("tconst", StringType(), False),
    StructField("titleType", StringType(), True),
    StructField("primaryTitle", StringType(), True),
    StructField("originalTitle", StringType(), True),
    StructField("isAdult", BooleanType(), True),
    StructField("startYear", IntegerType(), True),
    StructField("endYear", IntegerType(), True),
    StructField("runtimeMinutes", IntegerType(), True),
    StructField("genres", StringType(), True)
])


title_principals_schema = StructType([
    StructField("tconst", StringType(), False),
    StructField("ordering", IntegerType(), False),
    StructField("nconst", StringType(), False),
    StructField("category", StringType(), True),   
    StructField("job", StringType(), True), 
    StructField("characters", StringType(), True)
])
