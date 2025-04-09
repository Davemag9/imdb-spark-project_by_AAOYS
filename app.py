from pyspark.sql import SparkSession
from pyspark.sql.functions import split

from src.business_cases import get_directors_worked_with_Hanks, get_episodes_summary_per_season, get_top_3_movies_per_genre, \
    get_top_actors_by_high_rated_movies_count, get_top_countries_with_high_rated_movies, get_top_genres_by_avg_rating_last_10_years, \
    get_top_movies_runtime_per_genre, get_top_rated_recent_films, get_most_common_actor_pairs, get_top_lead_actors_after_2000, \
    get_top_3_movies_per_year, get_most_productive_writers, get_long_high_rated_movies, get_successful_directors, get_avg_runtime_by_genre, \
    count_good_movies_by_year, get_coactors_with_dicaprio, get_bottom_3_by_year, get_actors_with_strong_debut, find_man_in_characters, \
    count_films_per_person, add_total_principals_count, rank_principals_in_film, find_films_without_writers, count_films_with_without_directors, count_entries_per_film
from src.data_loader import load_data
from src.schemas import title_episode_schema, title_ratings_schema, title_akas_schema, title_crew_schema, \
    title_basics_schema, title_principals_schema, name_basics_schema, country_codes_schema

spark = SparkSession.builder \
    .appName("MyApp") \
    .config("spark.driver.extraJavaOptions", "-XX:ReservedCodeCacheSize=256m") \
    .config("spark.worker.cleanup.enabled", "true") \
    .config("spark.worker.cleanup.interval", "1800") \
    .config("spark.sql.cache.cleanup.interval", "600") \
    .getOrCreate()

title_ratings_df = load_data("./imdb_dataset/title.ratings.tsv", title_ratings_schema)
title_episode_df = load_data("./imdb_dataset/title.episode.tsv", title_episode_schema)

title_akas_df = load_data("./imdb_dataset/title.akas.tsv", title_akas_schema)
title_crew_df = load_data("./imdb_dataset/title.crew.tsv", title_crew_schema)

title_basics_df = load_data("./imdb_dataset/title.basics.tsv", title_basics_schema)
title_principals_df = load_data("./imdb_dataset/title.principals.tsv", title_principals_schema)

name_basics_df = load_data("./imdb_dataset/name.basics.tsv", name_basics_schema)
country_codes_df  = load_data("./imdb_dataset/slim-2.csv", country_codes_schema, delimiter=",")

# split cols with "," to array
title_crew_df = title_crew_df.withColumn("directors", split(title_crew_df["directors"], ","))
title_crew_df = title_crew_df.withColumn("writers", split(title_crew_df["writers"], ","))
title_basics_df = title_basics_df.withColumn("genres", split(title_basics_df["genres"], ","))
title_akas_df = title_akas_df.withColumn("types", split(title_akas_df["types"], ","))
title_akas_df = title_akas_df.withColumn("attributes", split(title_akas_df["attributes"], ","))

# business cases
#ALINA
# get_long_high_rated_movies(title_basics_df, title_ratings_df)
# get_successful_directors(name_basics_df, title_basics_df, title_ratings_df, title_crew_df)
# get_avg_runtime_by_genre(title_basics_df)
# count_good_movies_by_year(title_basics_df, title_ratings_df)
# get_bottom_3_by_year(title_basics_df, title_ratings_df)
# get_actors_with_strong_debut(name_basics_df, title_principals_df, title_basics_df, title_ratings_df)

#ANASTASIA
# get_top_genres_by_avg_rating_last_10_years(title_basics_df, title_ratings_df)
# get_top_countries_with_high_rated_movies(title_basics_df, title_akas_df, title_ratings_df, country_codes_df)
# get_top_3_movies_per_genre(title_basics_df, title_ratings_df)
# get_top_actors_by_high_rated_movies_count(title_basics_df, title_principals_df, title_ratings_df, name_basics_df)
# get_episodes_summary_per_season(title_episode_df, title_ratings_df, title_basics_df)
# get_top_movies_runtime_per_genre(title_basics_df, title_ratings_df)
                                         
#OLEH
# get_directors_worked_with_Hanks(name_basics_df, title_principals_df, title_basics_df, title_crew_df) # 2m
# get_top_rated_recent_films(title_basics_df, title_ratings_df) # 1m
# get_most_common_actor_pairs(title_principals_df, title_basics_df, name_basics_df) # 6m
# get_top_lead_actors_after_2000(title_principals_df, title_basics_df, name_basics_df) # 1m
# get_top_3_movies_per_year(title_basics_df, title_ratings_df) # 1m
# get_most_productive_writers(title_basics_df, title_crew_df, name_basics_df) # 2m

#YURA
# find_man_in_characters(title_principals_df)
# count_films_per_person(title_principals_df)
# add_total_principals_count(title_principals_df)
# rank_principals_in_film(title_principals_df)
# find_films_without_writers(title_principals_df)
# count_films_with_without_directors(title_principals_df)
# count_entries_per_film(title_principals_df)

# clean up
# spark.sql("CLEAR CACHE")
# spark.catalog.clearCache()
# spark.sparkContext.setCheckpointDir("")
spark.stop()
