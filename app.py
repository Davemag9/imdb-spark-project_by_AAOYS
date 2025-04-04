from data_loader import load_data
from schemas import title_episode_schema, title_ratings_schema

title_ratings_df = load_data("./imdb_dataset/title.ratings.tsv", title_ratings_schema)
title_episode_df = load_data("./imdb_dataset/title.episode.tsv", title_episode_schema)