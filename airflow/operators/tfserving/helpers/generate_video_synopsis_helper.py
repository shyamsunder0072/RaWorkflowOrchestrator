#!/usr/bin/env python
# coding: utf8

"""
    Generate Video Synopsis Task Helper.


"""

from airflow.operators.tfserving.base_classes.base_helper import BaseHelper
from airflow.operators.tfserving.ioutils import conf as params
from airflow.operators.tfserving.ioutils.tf_serving_module import map_compute_fn


class GenerateVideoSynopsisHelper(BaseHelper):
    """ A wrapper class for using generate video synopsis task helper methods. """

    def __init__(self):
        """ Default constructor. """
        super(BaseHelper, self).__init__()
        return

    def get_tags(self, dir_list, max_duration_threshold,
                 compute_cores=params.three_tier_compute_cores,
                 compute_strategy=params.default_compute_strategy.value,
                 input_extensions=params.media_type['video']):
        """ A helper method to call commons function.
            Handles directory level parallelization as well.
        :param input_extensions: List of input file extensions
        :param dir_list: List of input directories containing input files.
        :param max_duration_threshold: Maximum duration of segment for video synopsis.
        :param compute_cores: Number of cores to run the task
        :param compute_strategy: Strategy to use for the task
        """
        mapped_fn = map_compute_fn(params.TFServingComputeFunctions.VIDEO_SYNOPSIS_GENERATOR.value)
        synchronous = True
        self.run_helper(dir_list, input_extensions, mapped_fn, synchronous,
                        compute_cores, compute_strategy, max_duration_threshold=max_duration_threshold)
