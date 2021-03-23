#!/usr/bin/env python
# coding: utf8

"""
    Color Histogram Feature extraction Helper.

    Extracts color histograms from images.

"""

from airflow.operators.tfserving.base_classes.base_helper import BaseHelper
from airflow.operators.tfserving.ioutils import conf as params
from airflow.operators.tfserving.ioutils.tf_serving_module import map_compute_fn


class ComputeColorHistogramFeaturesHelper(BaseHelper):
    """ A wrapper class for using object detection task helper methods. """

    def __init__(self):
        """ Default constructor. """
        super(BaseHelper, self).__init__()
        return

    def get_features(self, dir_list, histogram_type, n_bins,
                     compute_cores=params.three_tier_compute_cores,
                     compute_strategy=params.strategy_parallel,
                     input_extensions=params.media_type['image']):
        """ A helper method to call commons function.
            Handles directory level parallelization as well.
        :param dir_list: List of input directories containing input files.
        :param histogram_type: Compute histogram in the 'bgr' or 'hsv' space.
        :param n_bins: Number of bins for histogram
        :param compute_cores: Number of cores to use for computation.
        :param compute_strategy: Strategy to process the dir list
        :param input_extensions: List of file extensions
        """
        mapped_fn = map_compute_fn(params.TFServingComputeFunctions.COLOR_HISTOGRAM_COMPUTATION.value)
        synchronous = True
        return self.run_helper(dir_list, input_extensions,
                               mapped_fn, synchronous,
                               histogram_type=histogram_type,
                               n_bins=n_bins,
                               compute_cores=compute_cores,
                               compute_strategy=compute_strategy)
