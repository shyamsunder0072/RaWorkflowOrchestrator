#!/usr/bin/env python
# coding: utf8

"""
    Extract Key Frames Task Helper.


"""

from airflow.operators.tfserving.base_classes.base_helper import BaseHelper
from airflow.operators.tfserving.ioutils import conf as params
from airflow.operators.tfserving.ioutils.tf_serving_module import map_compute_fn


class ExtractKeyFramesHelper(BaseHelper):
    """ A wrapper class for using extract key frames task helper methods. """

    def __init__(self):
        """ Default constructor. """
        super(BaseHelper, self).__init__()
        return

    def get_tags(self, dir_list, scene_file_path, output_path, fps,
                 algo=params.FramesExtractionAlgo.HIST,
                 compute_cores=params.three_tier_compute_cores,
                 compute_strategy=params.default_compute_strategy.value,
                 input_extensions=params.media_type['video']):
        """ A helper method to call commons function.
            Handles directory level parallelization as well.
        :param dir_list: List of input directories containing input files.
        :param scene_file_path
        :param output_path: Audio level output paths.
        :param algo: Key Frame Extraction algorithm to use.
        :param fps: fps parameter
        :param compute_cores: Number of cores to run task
        :param compute_strategy: Strategy of task.
        :param input_extensions: List of file extensions
        """
        if algo.value == params.FramesExtractionAlgo.HIST.value:
            mapped_fn = map_compute_fn(params.TFServingComputeFunctions.KEY_FRAMES_EXTRACTION_HIST.value)
            synchronous = True
            return self.run_helper(dir_list, input_extensions,
                                   mapped_fn, synchronous,
                                   compute_cores,
                                   compute_strategy,
                                   output_path=output_path,
                                   scene_info_file=scene_file_path
                                   )
        elif algo.value == params.FramesExtractionAlgo.FPS.value:
            mapped_fn = map_compute_fn(params.TFServingComputeFunctions.KEY_FRAMES_EXTRACTION_FPS.value)
            synchronous = True
            return self.run_helper(dir_list, input_extensions,
                                   mapped_fn, synchronous,
                                   compute_cores,
                                   compute_strategy,
                                   fps=fps,
                                   scene_info_file=scene_file_path,
                                   output_path=output_path
                                   )
