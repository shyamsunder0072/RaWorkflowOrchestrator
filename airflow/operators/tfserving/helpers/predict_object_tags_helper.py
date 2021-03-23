#!/usr/bin/env python
# coding: utf8

"""
    Object tags prediction Task Helper.

    Runs prediction over video-scenes frames and combine them to store video-level tags.

"""

from airflow.operators.tfserving.base_classes.base_helper import BaseHelper
from airflow.operators.tfserving.ioutils import conf as params
from airflow.operators.tfserving.ioutils.tf_serving_module import map_tfserving_model


class PredictObjectTagsHelper(BaseHelper):
    """ A wrapper class for using object tags prediction task helper methods. """

    def __init__(self):
        """ Default constructor. """
        super(BaseHelper, self).__init__()
        return

    def get_tags(self, dir_list, return_seg_map=False,
                 compute_cores=params.three_tier_compute_cores,
                 compute_strategy=params.strategy_parallel,
                 input_extensions=params.media_type['image']):
        """ A helper method to call commons function.
            Handles directory level parallelization as well.
        :param dir_list: List of input directories containing input files.
        :param return_seg_map: Boolean to return the segmentation map.
        :param compute_cores
        :param compute_strategy
        :param input_extensions: List of input file extensions
        """
        mapped_fn = map_tfserving_model(params.TFServingModels.SEMANTIC_SEGMENTATION.value)
        synchronous = True
        return self.run_helper(dir_list, input_extensions,
                               mapped_fn, synchronous,
                               compute_cores=compute_cores,
                               compute_strategy=compute_strategy,
                               return_seg_map=return_seg_map
                               )
