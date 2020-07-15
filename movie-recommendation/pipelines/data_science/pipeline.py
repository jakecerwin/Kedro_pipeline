from kedro.pipeline import node, Pipeline
from typing import Dict, List
from movie_recommendation.pipelines.data_science.nodes import (
    split_data,
    train_model,
)


def create_pipeline(**kwargs):
    return Pipeline(
        [
            node(
                func=split_data,
                inputs=["ratings_table", "viewed_table", "parameters"],
                outputs=["train_table", "test_table", "evaluation_table"],
            ),
            node(
                func=train_model,
                inputs=["train_table", "viewed_table"],
                outputs="prediction_table",
            ),
        ],
        tags=["ds_tag"],
    )