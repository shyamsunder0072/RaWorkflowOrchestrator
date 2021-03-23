#!/usr/bin/env python
# coding: utf8

"""
    Extract Audio Embedding Task Helper.

    Runs audio embedding extraction at video level.

"""

from airflow.operators.tfserving.base_classes.base_helper import BaseHelper
from airflow.operators.tfserving.ioutils import conf as params
from airflow.operators.tfserving.ioutils.tf_serving_module import map_tfserving_model


class ExtractAudioEmbeddingsHelper(BaseHelper):
    """ A wrapper class for using extract audio embeddings task helper methods. """

    def __init__(self):
        """ Default constructor. """
        super(BaseHelper, self).__init__()
        return

    def get_tags(self, dir_list,
                 input_extensions=params.media_type['audio'],
                 compute_cores=params.three_tier_compute_cores,
                 compute_strategy=params.strategy_parallel):
        """ A helper method to call commons function.
            Handles directory level parallelization as well.
        :param dir_list: List of input directories containing input files.
        :param input_extensions: List of file extension
        :param compute_cores:
        :param compute_strategy:
        """
        mapped_fn = map_tfserving_model(params.TFServingModels.AUDIO_EMBEDDINGS_EXTRACTION.value)
        synchronous = True
        return self.run_helper(dir_list, input_extensions,
                               mapped_fn, synchronous,
                               compute_cores=compute_cores,
                               compute_strategy=compute_strategy)
