import pandas as pd
from functools import wraps
from typing import Callable
import time
import logging
import math

def log_running_time(func: Callable) -> Callable:
    """Decorator for logging node execution time.

        Args:
            func: Function to be executed.

        Returns:
            Decorator for logging the running time.

    """

    @wraps(func)
    def with_time(*args, **kwargs):
        log = logging.getLogger(__name__)
        t_start = time.time()
        result = func(*args, **kwargs)
        t_end = time.time()
        elapsed = t_end - t_start
        log.info("Running %r took %.2f seconds", func.__name__, elapsed)
        return result

    return with_time


@log_running_time
def create_ratings_table(ratings: pd.DataFrame) -> pd.DataFrame:
    """Preprocess the data for ratings.

        Args:
            ratings: Source data.
        Returns:
            ratings in a table with user rows and movie columns .

    """

    randomization_key = 10

    ratings = ratings.drop(columns='time').drop_duplicates()
    #ratings = ratings.query('rating >=3')
    ratings.reset_index(drop=True, inplace=True)

    #only consider movies with over n ratings
    n = 3000
    movies = ratings.movieid.value_counts()
    movies = movies[movies>n].index.tolist()

    #only consider ratings from users who have rated over n movies
    n = 10
    users = ratings.userid.value_counts()
    users = users[users>n].index.tolist()

    ratings = ratings.query('userid in @users')

    ratings = ratings.query('movieid in @movies')
    ratings.reset_index(drop=True, inplace=True)

    #transform data into matrix
    movies = set()
    users = dict(dict())

    for i in range(len(ratings)):
        user = ratings.loc[i, 'userid']
        movie = ratings.loc[i, 'movieid']
        rating = ratings.loc[i, 'rating']

        movies.add(movie)
        if user in users:
            users[user].update({movie: rating})
        else:
            users.update({user : dict({movie: rating})  })

    # construct dataframe
    df = pd.DataFrame.from_dict(users, orient='index')
    df.fillna(value=0.0, inplace=True)

    return df




@log_running_time
def create_viewed_table(ratings_table: pd.DataFrame) -> pd.DataFrame:
    """ Creates True/false table representing whether a user has watched each movie
        with identical rows and columns to the ratings table.

        Args:
            ratings_table: Preprocessed data for ratings stored in a dataframe
        Returns:
            table of 1/0 viewership.

    """

    return ratings_table.apply(lambda x: x > 0.0).astype(int)



