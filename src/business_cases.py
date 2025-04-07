from pyspark.sql.functions import explode, count, col, row_number, split
from pyspark.sql import functions as F, Window

from src.get_services import get_id_by_name, get_films_by_actor, get_movies_list, get_movies_after_year


def get_directors_worked_with_Hanks(name_basics_df, title_principals_df, title_basics_df, title_crew_df):
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



def get_top_lead_actors_after_2000(title_principals_df, title_basics_df, name_basics_df):
    movies_after_2000 = get_movies_after_year(2000, title_basics_df)

    lead_roles = title_principals_df \
        .filter(col("category").isin("actor", "actress")) \
        .filter(col("ordering") == 1) \
        .join(movies_after_2000, "tconst") \
        .groupBy("nconst") \
        .agg(count("*").alias("lead_roles_count"))

    result = lead_roles \
        .join(name_basics_df, "nconst") \
        .select("primaryName", "lead_roles_count") \
        .orderBy(col("lead_roles_count").desc())

    result.show(10, truncate=False)
    return result


def get_top_3_movies_per_year(title_basics_df, ratings_df):
    movies_with_ratings = title_basics_df \
        .filter(col("titleType") == "movie") \
        .join(ratings_df, "tconst") \
        .filter(col("startYear").isNotNull())

    window_spec = Window.partitionBy("startYear").orderBy(col("averageRating").desc())

    ranked = movies_with_ratings.withColumn("rank", row_number().over(window_spec))

    top_3_per_year = ranked.filter(col("rank") <= 3) \
        .select("startYear", "primaryTitle", "averageRating") \
        .orderBy(col("startYear").desc(), col("averageRating").desc())

    top_3_per_year.show(100, truncate=False)
    return top_3_per_year


def get_most_productive_writers(title_basics_df, title_crew_df, name_basics_df):
    movies = get_movies_list(title_basics_df)

    writers = title_crew_df.join(movies, "tconst") \
        .withColumn("writer_id", explode(col("writers"))) \
        .select("writer_id")

    writer_counts = writers.groupBy("writer_id") \
        .agg(count("*").alias("num_movies")) \
        .orderBy(col("num_movies").desc())

    writer_names = writer_counts.join(name_basics_df, writer_counts.writer_id == name_basics_df.nconst) \
        .select(col("primaryName").alias("Writer"), "num_movies") \
        .orderBy(col("num_movies").desc())

    writer_names.show(10, truncate=False)
    return writer_names