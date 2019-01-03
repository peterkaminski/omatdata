#!/usr/bin/env python

# Simple proof-of-concept script to insert a movie into the Movies
# table of an Airtable base. Also sets up Directors and Actors
# objects.

import os
from airtable import Airtable # pip install airtable-python-wrapper

airtable_api_key = os.environ['AIRTABLE_API_KEY']
airtable_base_movies = os.environ['AIRTABLE_BASE_MOVIES']

movies = Airtable(airtable_base_movies, 'Movies', api_key=airtable_api_key)
directors = Airtable(airtable_base_movies, 'Directors', api_key=airtable_api_key)
actors = Airtable(airtable_base_movies, 'Actors', api_key=airtable_api_key)

data = {
  "Title": "Cloud Atlas",
  "Year": 2012,
  "Rated": "R",
  "Runtime": 172,
  "Genre": "Action",
  "Plot": "An exploration of how the actions of individual lives impact one another in the past, present and future, as one soul is shaped from a killer into a hero, and an act of kindness ripples across centuries to inspire a revolution.",
  "Poster": [{"url":"https://m.media-amazon.com/images/M/MV5BMTczMTgxMjc4NF5BMl5BanBnXkFtZTcwNjM5MTA2OA@@._V1_SX300.jpg"}],
  "Metascore": 55,
  "IMDB Rating": 7.5,
  "IMDB Votes": 319919,
  "IMDB URL": "https://www.imdb.com/title/tt1371111/",
  "Box Office": 22100000,
  "Production": "Warner Bros. Pictures",
  "OMDb Metadata": True
}

movies.insert(data)
