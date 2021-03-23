#!/usr/bin/env python
# coding: utf8

"""
    Pose Detection Task Helper.

"""

from airflow.operators.tfserving.base_classes.base_helper import BaseHelper
from airflow.operators.tfserving.ioutils import conf as params
from airflow.operators.tfserving.ioutils.tf_serving_module import map_tfserving_model


class PoseDetectionHelper(BaseHelper):
    """ A wrapper class for using pose detection task helper methods. """

    def __init__(self):
        """ Default constructor. """
        super(BaseHelper, self).__init__()
        return

    def get_tags(self, dir_list, model_backend, detection_type, output_stride, max_pose_detections,
                 compute_cores=params.three_tier_compute_cores, compute_strategy=params.strategy_parallel,
                 input_extensions=params.media_type['image']):
        """ A helper method to call commons function.
            Handles directory level parallelization as well.
        :param dir_list: List of input directories containing input files.
        :param model_backend: Model architecture to use as backend.
        :param detection_type: Type of pose detection.
        :param output_stride: Output stride to use in model architecture.
        :param max_pose_detections: Maximum number of poses to detect when using multi detection_type.
        :param compute_cores: Number of cores to use for computation.
        :param compute_strategy: Strategy to process the dir list
        :param input_extensions: List of file extensions
        """
        mapped_fn = map_tfserving_model(params.TFServingModels.POSE_DETECTION.value)
        synchronous = True
        return self.run_helper(dir_list, input_extensions,
                               mapped_fn, synchronous,
                               model_backend=model_backend,
                               output_stride=output_stride,
                               detection_type=detection_type,
                               max_pose_detections=max_pose_detections,
                               compute_cores=compute_cores,
                               compute_strategy=compute_strategy)
