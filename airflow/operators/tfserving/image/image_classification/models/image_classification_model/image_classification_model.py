#!/usr/bin/env python
# coding: utf8

""" Image Classification Model - Predict classes from images.

    Loads a sample image and obtains classes using a trained model.

"""

import os
import cv2
import numpy as np

os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3'

import tensorflow as tf
from airflow.operators.tfserving.base_classes.base_tf_model import BaseTFModel
from airflow.operators.tfserving.image.image_classification.models.image_classification_model.conf import \
    classification_models
from airflow.operators.tfserving.image.image_classification.models.image_classification_model.\
    image_classification_model_helper import ImageClassificationHelper

# disable eager execution which is enabled by default in tf>=2.0
tf.compat.v1.disable_eager_execution()


class ImageClassificationModel(BaseTFModel):
    """class to run inference for image classification model"""

    def __init__(self, model_name):
        """make a tf serving client."""
        self._model_config = classification_models[model_name]
        print(self._model_config)
        self._MODEL = self._model_config['model_name']
        self._VERSION = self._model_config['version']
        self.image_classes = ImageClassificationHelper().load_image_classification_classmap(self._MODEL)
        super().__init__(self._MODEL, self._VERSION)
        return

    def pre_process(self, input_data):
        image = cv2.imread(input_data)
        resized_image = cv2.resize(image, (self._model_config['image_width'], self._model_config['image_height']))
        resized_image = np.expand_dims(resized_image, axis=0)
        normalized_image = resized_image/255.0
        req_data = {
            self._TENSOR_NAME: self._model_config['tensor_name'],
            self._TENSOR_DTYPE: self._model_config['tensor_type'],
            self._INPUT_DATA: normalized_image
        }
        return req_data

    def post_process(self, input_data, predictions):
        predicted_scores = predictions[self._model_config['out_tensor_name']][0]
        prediction_classes = {obj: score for obj, score in zip(self.image_classes, predicted_scores)}
        return prediction_classes
