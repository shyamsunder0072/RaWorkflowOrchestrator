#!/usr/bin/env python
# coding: utf8

"""
    Compute RGB Streams Task Helper.


"""

from airflow.operators.tfserving.base_classes.base_helper import BaseHelper
from airflow.operators.tfserving.ioutils import conf as params
from airflow.operators.tfserving.ioutils.tf_serving_module import map_compute_fn


class ComputeRgbStreamsHelper(BaseHelper):
    """ A wrapper class for using compute rgb streams task helper methods. """

    def __init__(self):
        """ Default constructor. """
        super(BaseHelper, self).__init__()
        return

    def get_tags(self, dir_list, scene_file_path, stream_dir, scene_level_object_list,
                 compute_cores=params.three_tier_compute_cores,
                 compute_strategy=params.default_compute_strategy.value,
                 input_extensions=params.media_type['video']):
        """ A helper method to call commons function.
            Handles directory level parallelization as well.
        :param dir_list: List of input directories containing input files.
        :param scene_file_path
        :param stream_dir: Rgb streams output directory.
        :param scene_level_object_list
        :param compute_cores: Number of cores to run the task
        :param compute_strategy: Strategy to use for the task
        :param input_extensions: File extensions
        """
        # mapped_fn = run_rgb_streams_computation
        mapped_fn = map_compute_fn(params.TFServingComputeFunctions.RGB_STREAMS_COMPUTATION.value)
        synchronous = True
        return self.run_helper(dir_list, input_extensions,
                               mapped_fn, synchronous,
                               compute_cores,
                               compute_strategy,
                               scene_info_file=scene_file_path,
                               stream_dir=stream_dir,
                               scene_level_objects=scene_level_object_list)
