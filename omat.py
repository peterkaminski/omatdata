#!/usr/bin/env python

################################################################
#
# omat.py
#
# Given a file with a list of movie titles or IMDB URLs, inserts the
# movie metadata from OMDb into an Airtable base.  Loads the metadata
# from OMDb, *no* data from IMDB.
#
################################################################

import os # core
import ast # core
import argparse # pip install argparse
import omdb # pip install omdb
from airtable import Airtable # pip install airtable-python-wrapper

# Set up argparse
def init_argparse():
    parser = argparse.ArgumentParser(description='Load OMDb metadata into an Airtable base.')
    parser.add_argument('--movies', required=True, help='path to a text file containing names of movies, one per line')
    parser.add_argument('--fold-the', action='store_true', help='move "The" from beginning to end of title for better sorting')
    parser.add_argument('--include-adult', action='store_true', help='include movies with genre "Adult"')
    parser.add_argument('--set-field', action='append', help='specify a "key:value" to set a table field')
    parser.add_argument('--append-field', action='append', help='specify a "key:value" to append into table field')
    parser.add_argument('--verbose', action='store_true', help='print more information about process')
    return parser

# Load movie names
def load_movie_names(infile):
    with open(infile) as file:
        return file.readlines()

# Get movie IDs given movie name or IMDB URL
def get_movie_ids(name):
    # check for IMDB URL
    url_split = name.split('https://www.imdb.com/title/')
    if len(url_split) == 2:
        return {
            'included': [url_split[1].split('/')[0]],
            'excluded': [] }
    # otherwise use name
    name = name.rsplit(', The')[0].lower().replace(',', '').replace(':', '')
    movies = omdb.search_movie(name)
    return {
        'included':
          [ movie['imdb_id']
          for movie in movies
          if movie['title'].lower().replace(',', '').replace(':', '') == name
          or movie['title'].lower().replace(',', '').replace(':', '') == "the " + name ],
        'excluded':
          [ movie['imdb_id']
          for movie in movies
          if movie['title'].lower().replace(',', '').replace(':', '') != name
          and movie['title'].lower().replace(',', '').replace(':', '') != "the " + name ] }

# Upsert movie given movie data
def upsert_movie(args, movies_table, data):
    # set all --set-field fields
    for field in args.set_field:
        # next(iter(my_dict.items())) gets the first k,v from my_dict
        (k, v) = next(iter(ast.literal_eval('{'+field+'}').items()))
        data[k] = v
    # find existing record, if any
    record = movies_table.match('IMDB URL', data['IMDB URL'])
    if len(record) == 0:
        # insert
        movies_table.insert(data)
    else:
        # update while merging in --append-field fields
        record_fields = record['fields']
        # use dictionary unpacking to update `record` with `data`
        data = {**record_fields, **data}
        for field in args.append_field:
            # next(iter(my_dict.items())) gets the first k,v from my_dict
            (k, v) = next(iter(ast.literal_eval('{'+field+'}').items()))
            if k in data:
                data[k].append(v)
            else:
                data[k] = [v]
        movies_table.update(record['id'], data)

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
        name = name.replace('(co-director)', '') # for director's names
        records[name] = table.match(field, name)
        if len(records[name]) == 0:
            records[name] = table.insert({field: name})
    return [records[name]['id'] for name in records]

# Transform metadata from OMDb to Airtable columns
def transform_data(args, data):
    title = data['title']
    if args.fold_the and title.startswith('The '):
        title = title.replace('The ', '', 1) + ', The'
    return {
        'Title': "{} ({})".format(title, data['year']),
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
        'Production': data['production'] }

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
        movie_ids = get_movie_ids(movie_name.rstrip())
        # make sure --set-field and --append-field are lists, even if they're empty (for upsert_movie())
        if args.set_field is None:
            args.set_field = []
        if args.append_field is None:
            args.append_field = []
        for movie_id in movie_ids['included']:
            movie_data = get_movie_data(movie_id)
            if movie_data['genre'] == 'Adult' and not args.include_adult:
                continue
            print('  {} ({})'.format(movie_data['title'], movie_data['year']))
            movie_data['directors'] = get_multiple_records(directors_table, 'Name', movie_data['director'])
            movie_data['actors'] = get_multiple_records(actors_table, 'Name', movie_data['actors'])
            movie_data['genres'] = get_multiple_records(genres_table, 'Name', movie_data['genre'])
            data_transformed = transform_data(args, movie_data)
            upsert_movie(args, movies_table, data_transformed)
        if args.verbose:
            for movie_id in movie_ids['excluded']:
                print('  excluding {}'.format(movie_id))

# Run this script
if __name__ == "__main__":
    exit(main())
