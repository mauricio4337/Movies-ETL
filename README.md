# Movies-ETL

*Overview
---
The purpose of this module was to extract and transform data from different sources (Wikipedia and MovieLens).  Once the data was transformed, the data was loaded to a Postgres SQL data base.

*Extraction and transformation
---
   The Wikipedia data was given in a json format.  This data was readily loaded for into a dataframe.  The data, however, was formatted differently from column to column and within the columns.  Movie title information was recovered from multiple columns.   Box office and budget data was transformed from several different formats into a floating point number.  Release date data was converted into a single date_time format.  The running time of the movie was converted into a single time format.  The imdb id was extracted from the imdb url.
   The kaggle movie data was loaded from as csv.  The kaggle data was more consistent and required less manipulation.  Many of the same transformations on the Wikipedia data were available as needed to be applied to the kaggle data.  Once corresponding items were in the same formats for both sets of data, the dataframes were merged. Duplicate data was compared and the decision in each case was to keep the kaggle data, but to fill in any missing data with data from wikipedia.  
   The rating data was from the same source as the kaggle movie data.  The data was grouped by user id and a count was made of each rating for each movie.  

*Loading the data
---
  After the data was transformed a connection was made the Postgres SQL database movie_data.  The final movie dataframe was loaded into the table movies.  The rating data was too large to load at once, so it was loaded into a table in smaller chunks.
  The Challenge.py file contains all the code for the ETL process.
