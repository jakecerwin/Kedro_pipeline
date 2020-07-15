import numpy as np
import pandas as pd
from typing import Dict, List
from scipy.sparse.linalg import svds
import random
from functools import wraps
from typing import Callable
import time
import logging


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
def split_data(ratings_table: pd.DataFrame, viewed_table: pd.DataFrame, parameters: Dict) -> List:
    """Splits data into training and test sets.

        Args:
            ratings_table

        Returns:
            A list consisting of train, test, validation data

    """

    test_percentage = int(parameters["test_size"])
    validation_percentage = int(parameters["validation_size"])
    train_percentage = 100 - test_percentage - validation_percentage

    #r = random.Random()
    #r.seed(parameters["random_state"])

    seeding = pd.DataFrame(np.random.randint(0, 100, size=(ratings_table.shape)), columns=ratings_table.columns)

    inter = pd.DataFrame(np.multiply(seeding.to_numpy(copy=True), viewed_table.to_numpy(copy=True)),
                         columns=viewed_table.columns)

    train = inter.apply(lambda x: x < train_percentage).astype(int)
    test = inter.apply(lambda x: (train_percentage <= x) & (x < (100 - validation_percentage)) ).astype(int)
    validation = inter.apply(lambda x: (100 - validation_percentage) <= x).astype(int)

    train = pd.DataFrame(np.multiply(train.to_numpy(copy=True), ratings_table.to_numpy(copy=True)),
                         columns=ratings_table.columns)
    test = pd.DataFrame(np.multiply(test.to_numpy(copy=True), ratings_table.to_numpy(copy=True)),
                        columns=ratings_table.columns)
    validation = pd.DataFrame(np.multiply(validation.to_numpy(copy=True), ratings_table.to_numpy(copy=True)),
                              columns=ratings_table.columns)

    return [train, test, validation]


def train_model(train_table: pd.DataFrame, viewed_table: pd.DataFrame) -> pd.DataFrame:
    # construct numpy array
    data = train_table.to_numpy()
    user_ratings_mean = np.mean(data, axis=1)
    # factors in individual interpretation of the scale
    data_demeaned = data - user_ratings_mean.reshape(-1, 1)

    # use scipy sparse's svd to avoid 'killed: 9' memory issues
    U, sigma, Vt = svds(data_demeaned, k=50)

    sigma = np.diag(sigma)

    all_user_predicted_ratings = np.dot(np.dot(U, sigma), Vt) + user_ratings_mean.reshape(-1, 1)

    # give already viewed movies a rating of 0. Note these will still be taken ahead of adverse movies
    predictions = pd.DataFrame(np.multiply(all_user_predicted_ratings, viewed_table.to_numpy(copy=True)),
                               columns=train_table.columns)

    return predictions
