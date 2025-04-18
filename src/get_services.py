from pyspark.sql.functions import col


def get_id_by_name(name: str, names_df) -> str:
    id = names_df.filter(col("primaryName") == name).select("nconst").first()["nconst"]
    return id


def get_films_by_actor(actor_id: str, title_principals_df, title_basics_df):
    films = title_principals_df.filter(
        (col("nconst") == actor_id) & (col("category") == "actor")
    ).join(
        title_basics_df, title_principals_df.tconst == title_basics_df.tconst
    ).select(title_basics_df.tconst, title_basics_df.primaryTitle)

    films = films.dropDuplicates(["tconst"])
    return films


def get_movies_list(title_basics_df):
    movies = title_basics_df.filter(col("titleType") == "movie").select("tconst")
    return movies


def get_movies_after_year(year:int, title_basics_df):
    movies = title_basics_df \
        .filter((col("titleType") == "movie") & (col("startYear") > year)) \
        .select("tconst")
    return movies