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
def get_movie(movie_id):
    return omdb.imdbid(movie_id)

# safe integer
def safe_int(string):
    string = string.replace('$','').replace(',','').replace('N/A','').replace(' min','')
    int(string) if string != '' else 0

# safe float
def safe_float(string):
    string = string.replace('$','').replace(',','').replace('N/A','')
    float(string) if string != '' else 0

# Transform metadata from OMDb to Airtable columns
def transform_data(data):
    return {
        'Title': data['title'],
        'Year': int(data['year']),
        'Rated': data['rated'],
        'Runtime': safe_int(data['runtime'].replace(' min','')),
        'Genre': data['genre'],
        'Plot': data['plot'],
        'Poster': [{'url': data['poster']}],
        'Metascore': safe_int(data['metascore']),
        'IMDB Rating': safe_float(data['imdb_rating']),
        'IMDB Votes': safe_int(data['imdb_votes']),
        'IMDB URL': 'https://www.imdb.com/title/{}/'.format(data['imdb_id']),
        'Box Office': safe_int(data['box_office']),
        'Production': data['production'],
        'OMDb Metadata': True,
        'To Verify': True }

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

    # read movie names, insert movies
    for movie_name in load_movie_names(args.movies):
        print(movie_name.rstrip())
        movies = get_movies(movie_name.rstrip())
        for movie in movies['included']:
            print(' ', movie['title'])
            data_transformed = transform_data(get_movie(movie['imdb_id']))
            insert_movie(movies_table, data_transformed)

# Run this script
if __name__ == "__main__":
    exit(main())
