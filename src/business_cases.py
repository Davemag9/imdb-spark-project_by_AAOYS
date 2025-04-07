from pyspark.sql.functions import explode, count, col
from pyspark.sql import functions as F

from src.get_services import get_id_by_name, get_films_by_actor, get_movies_list


def get_directors_worked_with_Hanks(name_basics_df, title_principals_df, title_basics_df, title_crew_df):
    """
    Get the directors that worked with Tom Hanks
    """
    tom_hanks_id = get_id_by_name("Tom Hanks", name_basics_df)

    tom_hanks_titles = get_films_by_actor(tom_hanks_id, title_principals_df, title_basics_df)

    tom_hanks_directors = tom_hanks_titles.join(title_crew_df, "tconst") \
        .withColumn("director_id", explode(col("directors"))) \
        .select("director_id")

    top_directors = tom_hanks_directors.groupBy("director_id") \
        .agg(count("*").alias("movies_with_tom")) \
        .orderBy(col("movies_with_tom").desc())

    result = top_directors.join(name_basics_df, top_directors["director_id"] == name_basics_df["nconst"]) \
        .select("primaryName", "movies_with_tom") \
        .orderBy(col("movies_with_tom").desc())

    result.show(10, truncate=False)
    return result


def get_top_rated_recent_films(title_basics_df, title_ratings_df):
    result = title_basics_df.join(title_ratings_df, "tconst") \
        .filter((col("averageRating") > 8.0) & (col("startYear") > 2010)) \
        .select("primaryTitle", "startYear", "averageRating") \
        .orderBy(col("averageRating").desc())

    result.show(10, truncate=False)
    return result



def get_most_common_actor_pairs(title_principals_df, title_basics_df, name_basics_df):
    movies = get_movies_list(title_basics_df)

    actors = title_principals_df \
        .filter((col("category").isin("actor", "actress")) & (col("characters").isNotNull())) \
        .join(movies, "tconst") \
        .select("tconst", "nconst")

    actor_pairs = actors.alias("a1") \
        .join(actors.alias("a2"),
              (col("a1.tconst") == col("a2.tconst")) & (col("a1.nconst") < col("a2.nconst"))) \
        .select(
            col("a1.nconst").alias("actor1"),
            col("a2.nconst").alias("actor2")
        )

    pair_counts = actor_pairs.groupBy("actor1", "actor2") \
        .agg(count("*").alias("num_movies_together"))

    pair_with_names = pair_counts \
        .join(name_basics_df.select("nconst", "primaryName").alias("n1"),
              pair_counts.actor1 == F.col("n1.nconst")) \
        .join(name_basics_df.select("nconst", "primaryName").alias("n2"),
              pair_counts.actor2 == F.col("n2.nconst")) \
        .select(
            F.col("n1.primaryName").alias("Actor 1"),
            F.col("n2.primaryName").alias("Actor 2"),
            "num_movies_together"
        ) \
        .orderBy(col("num_movies_together").desc())

    pair_with_names.show(10, truncate=False)
    return pair_with_names
