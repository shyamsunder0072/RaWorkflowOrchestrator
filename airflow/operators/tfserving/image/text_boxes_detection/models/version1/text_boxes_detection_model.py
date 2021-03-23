#!/usr/bin/env python
# coding: utf8

""" OCR Detection Model - Text Cropping from frames.

    Loads a sample frame and obtains text cropping using a trained model.

"""

import os

os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3'

import tensorflow as tf
from airflow.operators.tfserving.base_classes.base_tf_model import BaseTFModel

from airflow.operators.tfserving.image.text_boxes_detection.models.version1 import \
    conf as model_config

# disable eager execution which is enabled by default in tf>=2.0
tf.compat.v1.disable_eager_execution()


class TextBoxesDetectionModel(BaseTFModel):
    """class to run inference for text boxes detection model"""

    _MODEL = model_config.MODEL_NAME
    _VERSION = model_config.MODEL_VERSION

    def __init__(self):
        """make a tf serving client."""
        super().__init__(self._MODEL, self._VERSION)
        return

    def pre_process(self, input_data):
        req_data = {
            self._TENSOR_NAME: 'input_1',
            self._TENSOR_DTYPE: tf.float32,
            self._INPUT_DATA: input_data
        }
        return req_data

    def post_process(self, input_data, predictions):
        box_groups = predictions['conv_cls.8']
        return box_groups
