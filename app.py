from pyspark.sql import SparkSession
from pyspark.sql.functions import split

from src.business_cases import get_directors_worked_with_Hanks
from src.data_loader import load_data
from src.get_services import get_id_by_name, get_films_by_actor
from src.schemas import title_episode_schema, title_ratings_schema, title_akas_schema, title_crew_schema, title_basics_schema, title_principals_schema,  name_basics_schema

spark = SparkSession.builder \
    .appName("MyApp") \
    .config("spark.driver.extraJavaOptions", "-XX:ReservedCodeCacheSize=256m") \
    .getOrCreate()


title_ratings_df = load_data("./imdb_dataset/title.ratings.tsv", title_ratings_schema)
title_episode_df = load_data("./imdb_dataset/title.episode.tsv", title_episode_schema)

title_akas_df = load_data("./imdb_dataset/title.akas.tsv", title_akas_schema)
title_crew_df = load_data("./imdb_dataset/title.crew.tsv", title_crew_schema)

title_basics_df = load_data("./imdb_dataset/title.basics.tsv", title_basics_schema)
title_principals_df = load_data("./imdb_dataset/title.principals.tsv", title_principals_schema)

name_basics_df = load_data("./imdb_dataset/name.basics.tsv", name_basics_schema)

# split cols with "," to array
title_crew_df = title_crew_df.withColumn("directors", split(title_crew_df["directors"], ","))
title_crew_df = title_crew_df.withColumn("writers", split(title_crew_df["writers"], ","))

# business cases
get_directors_worked_with_Hanks(name_basics_df, title_principals_df, title_basics_df, title_crew_df)