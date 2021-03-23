#!/usr/bin/env python
# coding: utf8

""" OCR Recognition Model - Text from cropped frames.

    Loads a cropped text frame and obtains text using a trained model.

"""

import os
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3'

import tensorflow as tf

from airflow.operators.tfserving.base_classes.base_tf_model import BaseTFModel
from airflow.operators.tfserving.image.optical_character_recognition.models.version1 import \
    conf as model_config

# disable eager execution which is enabled by default in tf>=2.0
tf.compat.v1.disable_eager_execution()


class OpticalCharacterRecognitionModel(BaseTFModel):
    """class to run inference for ocr recognition model"""

    _MODEL = model_config.MODEL_NAME
    _VERSION = model_config.MODEL_VERSION

    def __init__(self):
        """make a tf serving client."""
        super().__init__(self._MODEL, self._VERSION)
        return

    def pre_process(self, input_data):
        req_data = {
            self._TENSOR_NAME: 'input_2',
            self._TENSOR_DTYPE: tf.float32,
            self._INPUT_DATA: input_data
        }
        return req_data

    def post_process(self, input_data, predictions):
        text_decode = predictions['decode']
        return text_decode
