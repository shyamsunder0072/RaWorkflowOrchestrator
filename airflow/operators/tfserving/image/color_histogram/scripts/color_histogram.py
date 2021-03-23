#!/usr/bin/env python
# coding: utf8

from airflow.operators.tfserving.base_classes.base_module import BaseModule
from airflow.operators.tfserving.image.color_histogram.models import \
    ColorHistogramModel
from airflow.operators.tfserving.image.color_histogram.scripts import conf as params


class ColorHistogram(BaseModule):
    """class to return color histogram features from an image. """

    def __init__(self):
        self.model = ColorHistogramModel()
        super().__init__()

    def run_inference_per_input(self, frame_path, histogram_type, n_bins):
        hist = self.model.run((frame_path, histogram_type, n_bins))
        return {frame_path: hist}

    def run_color_histogram_feature_extraction(self, frames_list, histogram_type=params.default_histogram_type,
                                               n_bins=params.default_n_bins,
                                               compute_cores=1, synchronous=False,
                                               **kwargs):
        """ Performs object detection on frames and returns
        a list of parsed objects with a confidence value.
        :param compute_cores:
        :param frames_list: A list of frames from a video.
        :param histogram_type: Type of histogram - BGR/RGB (default = 'bgr')
        :param n_bins: Number of bins to use
        :param synchronous: (Optional) Default True for batching.
        """
        return self.run_inference(frames_list, synchronous, compute_cores, histogram_type, n_bins)
