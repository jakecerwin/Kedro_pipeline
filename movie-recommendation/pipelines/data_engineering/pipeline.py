from kedro.pipeline import node, Pipeline
from movie_recommendation.pipelines.data_engineering.nodes import (
    create_ratings_table,
    create_viewed_table,
)

def create_pipeline(**kwargs):
    return Pipeline(
        [
            node(
                func=create_ratings_table,
                inputs="ratings",
                outputs="ratings_table",
                name="create_ratings_table",
            ),
            node(
                func=create_viewed_table,
                inputs="ratings_table",
                outputs="viewed_table",
                name="create_viewed_table",
            ),
        ],
        tags=["de_tag"],
    )