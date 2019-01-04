#!/usr/bin/env python

################################################################
#
# omat.py
#
# Given a file with a list of movie titles, inserts the movie
# metadata from OMDb into an Airtable base.
#
################################################################

import os
import argparse # pip install argparse
import omdb # pip install omdb
from airtable import Airtable # pip install airtable-python-wrapper

# Set up argparse
def init_argparse():
    parser = argparse.ArgumentParser(description='Load OMDb metadata into an Airtable base.')
    parser.add_argument('--movies', required=True, help='path to a text file containing names of movies, one per line')
    parser.add_argument('--include-adult', action='store_true', help='include movies with genre "Adult"')
    return parser

# Load movie names
def load_movie_names(infile):
    with open(infile) as file:
        return file.readlines()

# Get movie IDs given movie name
def get_movies(movie_name):
    movies = omdb.search_movie(movie_name)
    return {
        'included': [ movie for movie in movies if movie['title'] == movie_name or movie['title'] == "The " + movie_name ],
        'excluded': [ movie for movie in movies if movie['title'] != movie_name and movie['title'] != "The " + movie_name ] }

# Insert movie given movie data
def insert_movie(movies_table, data):
    movies_table.insert(data)

# Get movie metadata given movie ID
def get_movie_data(movie_id):
    return omdb.imdbid(movie_id)

# safe integer
def safe_int(string):
    string = string.replace('$','').replace(',','').replace('N/A','').replace(' min','')
    return int(string) if string != '' else 0

# safe float
def safe_float(string):
    string = string.replace('$','').replace(',','').replace('N/A','')
    return float(string) if string != '' else 0

# get record IDs from an associated table given a string of names
# create new records if necessary
def get_multiple_records(table, field, names):
    records = {}
    for name in names.split(', '):
        records[name] = table.match(field, name)
        if len(records[name]) == 0:
            records[name] = table.insert({field: name})
    return [records[name]['id'] for name in records]

# Transform metadata from OMDb to Airtable columns
def transform_data(data):
    return {
        'Title': "{} ({})".format(data['title'], data['year']),
        'Directors': data['directors'],
        'Actors': data['actors'],
        'Year': int(data['year']),
        'Rated': data['rated'],
        'Runtime': safe_int(data['runtime'].replace(' min','')),
        'Genres': data['genres'],
        'Plot': data['plot'],
        'Poster': [{'url': data['poster']}],
        'Metascore': safe_int(data['metascore']),
        'IMDB Rating': safe_float(data['imdb_rating']),
        'IMDB Votes': safe_int(data['imdb_votes']),
        'IMDB URL': 'https://www.imdb.com/title/{}/'.format(data['imdb_id']),
        'Box Office': safe_int(data['box_office']),
        'Production': data['production'],
        'OmAtData': True,
        'Verified': False,
        'New': True }

def main():
    # get auth keys from environment
    omdb_api_key = os.environ['OMDB_API_KEY']
    airtable_api_key = os.environ['AIRTABLE_API_KEY']
    airtable_base_movies = os.environ['AIRTABLE_BASE_MOVIES']

    # get arguments
    argparser = init_argparse();
    args = argparser.parse_args();

    # set up OMDb connection
    omdb.set_default('apikey', omdb_api_key)

    # set up Airtable connections
    movies_table = Airtable(airtable_base_movies, 'Movies', api_key=airtable_api_key)
    directors_table = Airtable(airtable_base_movies, 'Directors', api_key=airtable_api_key)
    actors_table = Airtable(airtable_base_movies, 'Actors', api_key=airtable_api_key)
    genres_table = Airtable(airtable_base_movies, 'Genres', api_key=airtable_api_key)

    # read movie names, insert movies
    for movie_name in load_movie_names(args.movies):
        print(movie_name.rstrip())
        movies = get_movies(movie_name.rstrip())
        for movie in movies['included']:
            print(' ', movie['title'])
            movie_data = get_movie_data(movie['imdb_id'])
            if movie_data['genre'] == 'Adult' and not args.include_adult:
                continue
            movie_data['directors'] = get_multiple_records(directors_table, 'Name', movie_data['director'])
            movie_data['actors'] = get_multiple_records(actors_table, 'Name', movie_data['actors'])
            movie_data['genres'] = get_multiple_records(genres_table, 'Name', movie_data['genre'])
            data_transformed = transform_data(movie_data)
            insert_movie(movies_table, data_transformed)

# Run this script
if __name__ == "__main__":
    exit(main())
