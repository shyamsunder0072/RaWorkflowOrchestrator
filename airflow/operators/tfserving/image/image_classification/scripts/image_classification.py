#!/usr/bin/env python
# coding: utf8

from airflow.operators.tfserving.base_classes.base_module import BaseModule
from airflow.operators.tfserving.image.image_classification.models.image_classification_model.\
    image_classification_model import ImageClassificationModel
from airflow.operators.tfserving.image.image_classification.scripts import conf as params


class ImageClassification(BaseModule):
    """class to return image classes from an image. """

    def __init__(self):
        super().__init__()

    def run_inference_per_input(self, frame, image_classfication_model, confidence_threshold):
        prediction_map = image_classfication_model.run(frame)
        output = {}
        for k, v in prediction_map.items():
            if v >= confidence_threshold:
                output[k] = v
        return {frame: output}

    def run_image_classification(self, frames_list, model_name=params.default_model,
                                 confidence_threshold=params.confidence_threshold, compute_cores=1, synchronous=False,
                                 **kwargs):
        """ Performs image classification on frames and returns
        a list of parsed objects with a confidence value.
        :param compute_cores:
        :param frames_list: A list of frames from a video.
        :param model_name: Name of the image classification model to use.
        :param confidence_threshold:
        :param synchronous: (Optional) Default True for batching.
        """
        image_classification_model = ImageClassificationModel(model_name)
        return self.run_inference(frames_list, synchronous, compute_cores, image_classification_model,
                                  confidence_threshold)
