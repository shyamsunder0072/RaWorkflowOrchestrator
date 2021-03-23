#!/usr/bin/env python
# coding: utf8

from airflow.operators.tfserving.base_classes.base_module import BaseModule
from airflow.operators.tfserving.image.feature_extraction.models.feature_extraction_model.feature_extraction_model\
     import FeatureExtractionModel
from airflow.operators.tfserving.image.feature_extraction.scripts import conf as params


class FeatureExtraction(BaseModule):
    """class to return feature vectors from an image. """

    def __init__(self):
        super().__init__()

    def run_inference_per_input(self, frame, feature_extraction_model):
        feature_map = feature_extraction_model.run(frame)
        return {frame: feature_map}

    def run_feature_extraction(self, frames_list, model_name=params.default_model,
                               compute_cores=1, synchronous=False,
                               **kwargs):
        """ Returns a feature vector for a frame.
        :param compute_cores:
        :param frames_list: A list of frames from a video.
        :param model_name: Name of the image classification model to use.
        :param synchronous: (Optional) Default True for batching.
        """
        feature_extraction_model = FeatureExtractionModel(model_name)
        return self.run_inference(frames_list, synchronous, compute_cores, feature_extraction_model)
