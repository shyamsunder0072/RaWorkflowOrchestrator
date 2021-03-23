#!/usr/bin/env python
# coding: utf8

""" Feature Extraction Model - Extracts features from images.

    Loads a sample image and obtains features using a trained model.

"""

import os

import cv2
import numpy as np

os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3'

import tensorflow as tf
from airflow.operators.tfserving.base_classes.base_tf_model import BaseTFModel
from airflow.operators.tfserving.image.feature_extraction.models.feature_extraction_model.conf import \
    feature_extraction_models

# disable eager execution which is enabled by default in tf>=2.0
tf.compat.v1.disable_eager_execution()


class FeatureExtractionModel(BaseTFModel):
    """class to run inference for image classification model"""

    def __init__(self, model_name):
        """make a tf serving client."""
        self._model_config = feature_extraction_models[model_name]
        print(self._model_config)
        self._MODEL = self._model_config['model_name']
        self._VERSION = self._model_config['version']
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
        return predicted_scores
