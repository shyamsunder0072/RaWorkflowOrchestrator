#!/usr/bin/env python
# coding: utf8

"""
    Separate Audio Sources Task Helper.


"""

from airflow.operators.tfserving.base_classes.base_helper import BaseHelper
from airflow.operators.tfserving.ioutils import conf as params
from airflow.operators.tfserving.ioutils.tf_serving_module import map_tfserving_model


class SeparateAudioSourcesHelper(BaseHelper):
    """ A wrapper class for using separate audio sources task helper methods. """

    def __init__(self):
        """ Default constructor. """
        super(BaseHelper, self).__init__()
        return

    def get_tags(self, dir_list, output_paths,
                 compute_cores=params.three_tier_compute_cores,
                 compute_strategy=params.default_compute_strategy.value,
                 input_extensions=params.media_type['audio']):
        """ A helper method to call commons function.
            Handles directory level parallelization as well.
        :param dir_list: List of input directories containing input files.
        :param output_paths: Audio level output paths.
        :param compute_cores: Number of cores to run the task.
        :param compute_strategy: Strategy to use for the task.
        :param input_extensions
        """
        mapped_fn = map_tfserving_model(params.TFServingModels.SOURCE_SEPARATION.value)
        synchronous = True
        self.run_helper(dir_list, input_extensions, mapped_fn, synchronous,
                        compute_cores, compute_strategy, output_path=output_paths)
