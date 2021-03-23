#!/usr/bin/env python
# coding: utf8

"""
    Process Key Frames Task Helper.


"""

from airflow.operators.tfserving.base_classes.base_helper import BaseHelper
from airflow.operators.tfserving.ioutils import conf as params
from airflow.operators.tfserving.ioutils.tf_serving_module import map_compute_fn


class ProcessKeyFramesHelper(BaseHelper):
    """ A wrapper class for using process key frames task helper methods. """

    def __init__(self):
        """ Default constructor. """
        super(BaseHelper, self).__init__()
        return

    def get_tags(self, dir_list, output_dir_list,
                 compute_cores=params.three_tier_compute_cores,
                 compute_strategy=params.default_inner_compute_strategy,
                 input_extensions=params.media_type['image']):
        """ A helper method to call commons function.
            Handles directory level parallelization as well.
        :param dir_list: List of input directories containing input files.
        :param output_dir_list: List of output directories.
        :param compute_cores: Number of cores to run the task
        :param compute_strategy: Strategy to use for the task
        :param input_extensions
        """
        mapped_fn = map_compute_fn(params.TFServingComputeFunctions.FRAME_TEXT_REDUCTION.value)
        synchronous = True
        self.run_helper(dir_list, input_extensions, mapped_fn, synchronous,
                        compute_cores, compute_strategy, output_dir=output_dir_list)
