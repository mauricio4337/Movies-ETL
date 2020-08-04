# Movies-ETL

*Overview
---
The purpose of this module was to extract and transform data from different sources (Wikipedia and MovieLens).  Once the data was transformed, the data was loaded to a Postgres SQL data base.

*Extraction and transformation
---
The Wikipedia data was given in a json format.  This data was readily loaded for into a dataframe.  The data, however, was formatted differently from column to column and within the columns.  Movie title information was recovered from multiple columns.   Box office and budget data was transformed from several different formats into a floating point number.  Release date data was converted into a single date_time format.  The running time of the movie was converted into a single time format.  The imdb id was extracted from the imdb url.

The kaggle movie data was more consistent and required less manipulation.
