from pyspark.sql.functions import explode, count, col

from src.get_services import get_id_by_name, get_films_by_actor


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
