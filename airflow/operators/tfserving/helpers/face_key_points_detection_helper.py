#!/usr/bin/env python
# coding: utf8

"""
    Face Key Points Detection Task Helper.

"""

from airflow.operators.tfserving.base_classes.base_helper import BaseHelper
from airflow.operators.tfserving.ioutils import conf as params
from airflow.operators.tfserving.ioutils.tf_serving_module import map_tfserving_model


class FaceKeyPointsDetectionHelper(BaseHelper):
    """ A wrapper class for using face key points detection task helper methods. """

    def __init__(self):
        """ Default constructor. """
        super(BaseHelper, self).__init__()
        return

    def get_tags(self, dir_list, confidence_threshold, dump_aligned_images=False,
                 compute_cores=params.three_tier_compute_cores, compute_strategy=params.strategy_parallel,
                 input_extensions=params.media_type['image']):
        """ A helper method to call commons function.
            Handles directory level parallelization as well.
        :param dir_list: List of input directories containing input files.
        :param confidence_threshold
        :param dump_aligned_images
        :param compute_cores: Number of cores to use for computation.
        :param compute_strategy: Strategy to process the dir list
        :param input_extensions: List of file extensions
        """
        mapped_fn = map_tfserving_model(params.TFServingModels.FACE_KEY_POINTS_DETECTION.value)
        synchronous = True
        return self.run_helper(dir_list, input_extensions,
                               mapped_fn, synchronous,
                               confidence_threshold=confidence_threshold,
                               dump_aligned_images=dump_aligned_images,
                               compute_cores=compute_cores,
                               compute_strategy=compute_strategy)
