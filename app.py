from data_loader import load_data
from schemas import title_episode_schema, title_ratings_schema, title_akas_schema, title_crew_schema, title_basics_schema, title_principals_schema

title_ratings_df = load_data("./imdb_dataset/title.ratings.tsv", title_ratings_schema)
title_episode_df = load_data("./imdb_dataset/title.episode.tsv", title_episode_schema)

title_akas_df = load_data("./imdb_dataset/title.akas.tsv", title_akas_schema)
title_crew_df = load_data("./imdb_dataset/title.crew.tsv", title_crew_schema)

title_basics_df = load_data("./imdb_dataset/title.basics.tsv", title_basics_schema)
title_principals_df = load_data("./imdb_dataset/title.principals.tsv", title_principals_schema)