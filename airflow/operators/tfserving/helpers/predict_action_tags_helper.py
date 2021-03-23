#!/usr/bin/env python
# coding: utf8

"""
    Action tags prediction Task Helper.

    Runs prediction over video-scenes frames and combine them to store video-level tags.

"""

from airflow.operators.tfserving.base_classes.base_helper import BaseHelper
from airflow.operators.tfserving.ioutils import conf as params
from airflow.operators.tfserving.ioutils.tf_serving_module import map_tfserving_model


class PredictActionTagsHelper(BaseHelper):
    """ A wrapper class for using action tags prediction task helper methods. """

    def __init__(self):
        """ Default constructor. """
        super(BaseHelper, self).__init__()
        return

    def get_tags(self, dir_list,
                 compute_cores=params.three_tier_compute_cores,
                 compute_strategy=params.strategy_parallel,
                 input_extensions=params.media_type['rgb_stream']):
        """ A helper method to call commons function.
            Handles directory level parallelization as well.
        :param dir_list: List of input directories containing input files.
        :param compute_cores
        :param compute_strategy
        :param input_extensions
        """
        mapped_fn = map_tfserving_model(params.TFServingModels.ACTION_RECOGNITION.value)
        synchronous = True
        return self.run_helper(dir_list, input_extensions,
                               mapped_fn, synchronous,
                               compute_cores=compute_cores,
                               compute_strategy=compute_strategy)
