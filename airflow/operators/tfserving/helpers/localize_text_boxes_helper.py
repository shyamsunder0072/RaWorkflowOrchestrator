#!/usr/bin/env python
# coding: utf8

"""
    Optical Character Recognition Task Helper.

    Runs prediction over video-scenes frames and combine them to store video-level tags.

"""

from airflow.operators.tfserving.base_classes.base_helper import BaseHelper
from airflow.operators.tfserving.ioutils import conf as params
from airflow.operators.tfserving.ioutils.tf_serving_module import map_tfserving_model


class LocalizeTextBoxesHelper(BaseHelper):
    """ A wrapper class for using ocr tags prediction task helper methods. """

    def __init__(self):
        """ Default constructor. """
        super(BaseHelper, self).__init__()
        return

    def get_tags(self, dir_list, frame_width=640, frame_height=360, batch_size=8,
                 compute_cores=params.three_tier_compute_cores,
                 compute_strategy=params.strategy_parallel,
                 input_extensions=params.media_type['image']):
        """ A helper method to call commons function.
            Handles directory level parallelization as well.
        :param dir_list: List of input directories containing input files.
        :param frame_width: Width of input frame.
        :param frame_height: Height of input frame.
        :param batch_size: Number of images from a single input directory to batch
        :param compute_cores: Number of cores to use for computation.
        :param compute_strategy: Strategy to process the dir list
        :param input_extensions: List of file extensions
        """
        mapped_fn = map_tfserving_model(params.TFServingModels.TEXT_BOXES_DETECTION.value)
        synchronous = True
        return self.run_helper(dir_list, input_extensions,
                               mapped_fn, synchronous,
                               compute_cores=compute_cores,
                               compute_strategy=compute_strategy,
                               frame_width=frame_width,
                               frame_height=frame_height,
                               batch_size=batch_size
                               )
