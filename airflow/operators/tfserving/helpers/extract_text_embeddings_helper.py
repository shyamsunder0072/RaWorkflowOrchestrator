#!/usr/bin/env python
# coding: utf8

"""
    Extract Text Embedding Task Helper.

    Runs text embedding extraction at video level.

"""

from airflow.operators.tfserving.base_classes.base_helper import BaseHelper
from airflow.operators.tfserving.ioutils import conf as params
from airflow.operators.tfserving.ioutils.tf_serving_module import map_tfserving_model


class ExtractTextEmbeddingsHelper(BaseHelper):
    """ A wrapper class for using extract text embeddings task helper methods. """

    def __init__(self):
        """ Default constructor. """
        super(BaseHelper, self).__init__()
        return

    def get_tags(self, dir_list, item_col, text_col, category_col, categories,
                 compute_cores=params.three_tier_compute_cores,
                 compute_strategy=params.default_compute_strategy.value,
                 input_extensions=params.media_type['flat_file']):
        """ A helper method to call commons function.
            Handles directory level parallelization as well.
        :param dir_list: List of input directories containing input files.
        :param item_col: Video Id column name.
        :param text_col: Video Text column name.
        :param category_col: Category column name.
        :param categories: List of categories to run the task.
        :param input_extensions: List of file extensions.
        :param compute_cores: Number of cores to run the task.
        :param compute_strategy: Strategy to use for the task.
        """
        # Todo: Remove Category_col and Categories after merging tasks and code bricks.
        # Todo: Change default input extensions post merging.
        mapped_fn = map_tfserving_model(params.TFServingModels.TEXT_EMBEDDINGS_EXTRACTION.value)
        synchronous = True
        return self.run_helper(dir_list, input_extensions,
                               mapped_fn, synchronous,
                               compute_cores,
                               compute_strategy,
                               item_col=item_col,
                               text_col=text_col,
                               category_col=category_col,
                               categories_list=[categories]
                               )
