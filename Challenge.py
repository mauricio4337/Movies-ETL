

# Import dependencies
import json
import pandas as pd
import numpy as np
import re
from sqlalchemy import create_engine
from config import db_password
import time
import psycopqg2




def clean_movie(movie):
    movie = dict(movie) # make a non-destructive copy
    
    alt_titles = {}
    # Combine alternate titles into one list
    for key in ['Also known as','Arabic','Cantonese','Chinese','French',
                'Hangul','Hebrew','Hepburn','Japanese','Literally',
                'Mandarin','McCune–Reischauer','Original title','Polish',
                'Revised Romanization','Romanized','Russian',
                'Simplified','Traditional','Yiddish']:
        
        if key in movie:
            alt_titles[key] = movie[key]
            movie.pop(key)
            
        if len(alt_titles) > 0:
            movie['alt_titles'] = alt_titles
            
        # merge column names
    def change_column_name(old_name, new_name):
        if old_name in movie:
            movie[new_name] = movie.pop(old_name)
    change_column_name('Adaptation by', 'Writer(s)')
    change_column_name('Country of origin', 'Country')
    change_column_name('Directed by', 'Director')
    change_column_name('Distributed by', 'Distributor')
    change_column_name('Edited by', 'Editor(s)')
    change_column_name('Length', 'Running time')
    change_column_name('Original release', 'Release date')
    change_column_name('Music by', 'Composer(s)')
    change_column_name('Produced by', 'Producer(s)')
    change_column_name('Producer', 'Producer(s)')
    change_column_name('Productioncompanies ', 'Production company(s)')
    change_column_name('Productioncompany ', 'Production company(s)')
    change_column_name('Released', 'Release Date')
    change_column_name('Release Date', 'Release date')
    change_column_name('Screen story by', 'Writer(s)')
    change_column_name('Screenplay by', 'Writer(s)')
    change_column_name('Story by', 'Writer(s)')
    change_column_name('Theme music composer', 'Composer(s)')
    change_column_name('Written by', 'Writer(s)')
            
    return movie

# Define a function that performs all the necessary ETL steps for the wikipedia film data, kaggle film data, and ratings data
def extract_transform_load(wiki_film, kaggle_film, ratings_file):
    #load json data
    with open(wiki_film, mode='r') as file:
        wiki_movies_raw = json.load(file)
        
    #load kaggle film data and ratings data
    kaggle_metadata_df = pd.read_csv(kaggle_film)
    ratings_df = pd.read_csv(ratings_file)
    
    # Begin cleaning wiki_movies raw, keeping movies with imdb link only and eliminating tv shows
    wiki_movies = [movie for movie in wiki_movies_raw 
                   if ('Director' in movie or 'Directed by' in movie) 
                   and 'imdb_link' in movie 
                   and 'No. of episodes' not in movie]
    
    #Apply clean_movie function to the movies in wiki_movies and create wiki_movies_df from the output
    clean_movies = [clean_movie(movie) for movie in wiki_movies]
    wiki_movies_df = pd.DataFrame(clean_movies)

    # Extract the imdb id from 'imdb_link' in wiki_movies_df, drop duplicates
    try:
        wiki_movies_df['imdb_id'] = wiki_movies_df['imdb_link'].str.extract(r'(tt\d{7})')
        wiki_movies_df.drop_duplicates(subset='imdb_id', inplace=True)
    except Exception as (e):
        print(e)
        
    # keep the columns in wiki movies if the percent of null values in a column is less than 90%
    wiki_columns_to_keep = [column for column in wiki_movies_df.columns 
                            if wiki_movies_df[column].isnull().sum() < len(wiki_movies_df) * 0.9]
    wiki_movies_df = wiki_movies_df[wiki_columns_to_keep]
 
    # clean box office data
    box_office = wiki_movies_df['Box office'].dropna()
    box_office = box_office.apply(lambda x: ' '.join(x) if type(x) == list else x)

    form_one = r'\$\s*\d+\.?\d*\s*[mb]illi?on'
    form_two = r'\$\s*\d{1,3}(?:[,\.]\d{3})+(?!\s[mb]illion)'

    # Define parse_dollars to clean Box office and Budget data
    def parse_dollars(s):
    # if s in not a string, return NaN
        if type(s) != str:
            return np.nan
    
    # if input is of the form $###.# million
        if re.match(r'\$\s*\d+\.?\d*\s*milli?on',s, flags=re.IGNORECASE):
        # remove the $ sign and 'million'
            s = re.sub('\$|\s|[a-zA-Z]','',s)
        
        # covert to a float by multiplying by millions
            value = float(s)*10**6
            return value
        
        
    # if input is of the form $###.# billion
        elif re.match(r'\$\s*\d+\.?\d*\s*billi?on',s, flags=re.IGNORECASE):
        # remove the $ sign and 'billion'
            s = re.sub('\$|\s|[a-zA-Z]','',s)
        # covert to a float by multiplying by billions
            value = float(s)*10**9
            return value
    
    # if input is of the form $###,###,###
        elif re.match(r'\$\s*\d{1,3}(?:[,\.]\d{3})+(?!\s[mb]illi?on)', s, flags=re.IGNORECASE):
        
        # remove dollar sign and commas
            s = re.sub('\$|,','', s)
        # convert to a float
            value = float(s)
            return value
        
    # otherwise return NaN
        else:
            return np.nan
    
    wiki_movies_df['box_office'] = box_office.str.extract(f'({form_one}|{form_two})', flags=re.IGNORECASE)[0].apply(parse_dollars)
    wiki_movies_df.drop('Box office', axis=1, inplace=True)
    
    # Clean budget data
    budget = wiki_movies_df['Budget'].dropna()
    budget = budget.map(lambda x: ' '.join(x) if type(x) == list else x)
    budget = budget.str.replace(r'\$.*[-—–](?![a-z])', '$', regex=True)
    wiki_movies_df['budget'] = budget.str.extract(f'({form_one}|{form_two})', flags=re.IGNORECASE)[0].apply(parse_dollars)
    wiki_movies_df.drop('Budget', axis=1, inplace=True)
    
    # Clean release date data
    release_date = wiki_movies_df['Release date'].dropna().apply(lambda x: ' '.join(x) if type(x) == list else x)
    date_form_one = r'(?:January|February|March|April|May|June|July|August|September|October|November|December)\s[123]\d,\s\d{4}'
    date_form_two = r'\d{4}.[01]\d.[123]\d'
    date_form_three = r'(?:January|February|March|April|May|June|July|August|September|October|November|December)\s\d{4}'
    date_form_four = r'\d{4}'
    
    wiki_movies_df['release_date'] = pd.to_datetime(release_date.str.extract(f'({date_form_one}|{date_form_two}|{date_form_three}|{date_form_four})')[0], infer_datetime_format=True)
    
    # Extract movie run time data, drop Nan, extract list entries
    running_time = wiki_movies_df['Running time'].dropna().apply(lambda x: ' '.join(x) if type(x) == list else x)
    running_time_extract = running_time.str.extract(r'(\d+)\s*ho?u?r?s?\s*(\d*)|(\d+)\s*m')
    running_time_extract = running_time_extract.apply(lambda col: pd.to_numeric(col, errors='coerce')).fillna(0)
    wiki_movies_df['running_time'] = running_time_extract.apply(lambda row: row[0]*60 + row[1] if row[2] == 0 else row[2], axis=1)
    wiki_movies_df.drop('Running time', axis=1, inplace=True)
    
    # clean kaggle data
    kaggle_metadata_df = kaggle_metadata_df[kaggle_metadata_df['adult'] == 'False'].drop('adult',axis='columns')
    kaggle_metadata_df['video'] = kaggle_metadata_df['video'] == 'True'
    kaggle_metadata_df['budget'] = kaggle_metadata_df['budget'].astype(int)
    kaggle_metadata_df['id'] = pd.to_numeric(kaggle_metadata_df['id'], errors='raise')
    kaggle_metadata_df['popularity'] = pd.to_numeric(kaggle_metadata_df['popularity'], errors='raise')
    kaggle_metadata_df['release_date'] = pd.to_datetime(kaggle_metadata_df['release_date'])

    # merge wiki_movies_df and kaggle_metadata_df (inner join) on imdb_id
    movies_merge_df = pd.merge(wiki_movies_df, kaggle_metadata_df, on='imdb_id', suffixes=['_wiki','_kaggle'])
    # drop wiki columns that are redundant/have less reliable data
    movies_merge_df = movies_merge_df.drop(columns=['title_wiki','release_date_wiki','Language', 'Production company(s)'], axis = 1)
    
    # define function to replace missing kaggle data with wikipedia data then drop wikipedia column
    def fill_missing_kaggle_data(df, kaggle_column, wiki_column):
        df[kaggle_column] = df.apply(lambda row: row[wiki_column] if row[kaggle_column] == 0 else row[kaggle_column], axis=1)
        df.drop(columns=wiki_column, inplace=True)

    #Apply fill missing function to runtime, budget, and revenue columns
    fill_missing_kaggle_data(movies_merge_df, 'runtime', 'running_time')
    fill_missing_kaggle_data(movies_merge_df, 'budget_kaggle', 'budget_wiki')
    fill_missing_kaggle_data(movies_merge_df, 'revenue', 'box_office')
    
    #reorder columns grouped by similar type
    movies_merge_df = movies_merge_df.loc[:,['imdb_id','id','title_kaggle',
                                             'original_title','tagline','belongs_to_collection','url','imdb_link',
                                             'runtime','budget_kaggle','revenue','release_date_kaggle',
                                             'popularity','vote_average','vote_count','genres','original_language',
                                             'overview','spoken_languages','Country','production_companies','production_countries',
                                             'Distributor','Producer(s)','Director','Starring','Cinematography',
                                             'Editor(s)','Writer(s)','Composer(s)','Based on']]
    
    #rename columns
    movies_merge_df.rename({'id':'kaggle_id',
                  'title_kaggle':'title',
                  'url':'wikipedia_url',
                  'budget_kaggle':'budget',
                  'release_date_kaggle':'release_date',
                  'Country':'country',
                  'Distributor':'distributor',
                  'Producer(s)':'producers',
                  'Director':'director',
                  'Starring':'starring',
                  'Cinematography':'cinematography',
                  'Editor(s)':'editors',
                  'Writer(s)':'writers',
                  'Composer(s)':'composers',
                  'Based on':'based_on'
                 }, axis='columns', inplace=True)

    # Get a count of the number of users for each rating for each movie, rename userId as count
    rating_counts = ratings_df.groupby(['movieId','rating'], as_index=False).count().rename(
        {'userId':'count'}, axis=1).pivot(
        index='movieId',columns='rating', values='count')
    rating_counts.columns = ['rating_' + str(col) for col in rating_counts.columns] #add identifier to each column to prepare for merge
    # left merge with movies_merge_df
    movies_with_ratings_df = pd.merge(movies_merge_df, rating_counts, left_on='kaggle_id', right_index=True, how='left')
    movies_with_ratings_df[rating_counts.columns] = movies_with_ratings_df[rating_counts.columns].fillna(0)
    
    # create a connection to Postgres SQL database movie_data
    db_string = f"postgres://postgres:{db_password}@127.0.0.1:5432/movie_data"
    engine = create_engine(db_string)
    # create table movies in movie_data database from movies_merge_df
    movies_merge_df.to_sql(name='movies', con=engine, if_exists='append') 
    
    # Import rating data in chunks manageable chunks
    rows_imported = 0
    # get the start time from time.time()
    start_time = time.time()
    
    for data in pd.read_csv(f'{file_dir}\ratings.csv', chunksize=1000000):
        print(f'importing rows {rows_imported} to {rows_imported + len(data)}...', end='')
        
        # import data to ratings table in movie_data db
        data.to_sql(name='ratings', con=engine, if_exists='append')

        # increment the number of rows imported by the chunksize
        rows_imported += len('data')
    
        # print that the rows have finished importing with elapsed time
        print(f'Done. Total elapsed time: {time.time() - start_time} seconds')


# store file path for raw data files, file specific paths
file_dir = ".\Data"
wiki_film = f'{file_dir}\wikipedia.movies.json'
kaggle_film = f'{file_dir}\movies_metadata.csv'
ratings_file = f'{file_dir}\ratings.csv'

# Call ETL function
extract_transform_load(wiki_film, kaggle_film, ratings_file)


# Competing data:
# Wiki                     Movielens                Resolution
#--------------------------------------------------------------------------
# title_wiki               title_kaggle             Drop Wikipedia
# running_time             runtime                  Keep kaggle, fill in missing data with wikipedia
# budget_wiki              budget_kaggle            Keep kaggle, fill in missing data with wikipedia
# box_office               revenue                  Keep kaggle, fill in missing data with wikipedia
# release_date_wiki        release_date_kaggle      Drop Wikipedia
# Language                 original_language        Drop Wikipedia
# Production company(s)    production_companies     Drop Wikipedia

