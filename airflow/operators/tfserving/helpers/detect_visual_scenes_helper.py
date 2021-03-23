#!/usr/bin/env python
# coding: utf8

"""
    Detect Visual Scenes Task Helper.


"""

from airflow.operators.tfserving.base_classes.base_helper import BaseHelper
from airflow.operators.tfserving.ioutils import conf as params
from airflow.operators.tfserving.ioutils.tf_serving_module import map_compute_fn


class DetectVisualScenesHelper(BaseHelper):
    """ A wrapper class for using detect visual task helper methods. """

    def __init__(self):
        """ Default constructor. """
        super(BaseHelper, self).__init__()
        return

    def get_tags(self, dir_list, scene_info_files, minimum_scene_duration_list, maximum_scene_duration_list, algorithm,
                 dump_scenes=False, compute_cores=params.three_tier_compute_cores,
                 compute_strategy=params.default_compute_strategy.value,
                 input_extensions=params.media_type['video']):
        """ A helper method to call commons function.
            Handles directory level parallelization as well.
        :param dir_list: List of input directories containing input files.
        :param scene_info_files.
        :param minimum_scene_duration_list: Filter scenes out with duration less than thsi parameter.
        :param maximum_scene_duration_list: Maximum scene duration, beyond which the scene should be split. (set -1 to disable this feature)
        :param algorithm: Algorithm to use for visual scene detection
        :param dump_scenes: Boolean variable to control dumping of visual scenes
        :param compute_cores: Number of cores to run the task
        :param compute_strategy: Strategy to use for the task
        :param input_extensions: List of file extensions
        """
        mapped_fn = map_compute_fn(params.TFServingComputeFunctions.VIDEO_SCENE_DETECTION.value)
        synchronous = True
        self.run_helper(dir_list, input_extensions, mapped_fn, synchronous, compute_cores, compute_strategy,
                        scene_info_file=scene_info_files, algorithm=algorithm, minimum_scene_duration=minimum_scene_duration_list,
                        maximum_scene_duration=maximum_scene_duration_list, dump_scenes=dump_scenes)
