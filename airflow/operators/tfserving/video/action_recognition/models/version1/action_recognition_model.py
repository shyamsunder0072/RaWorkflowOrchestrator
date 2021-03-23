#!/usr/bin/env python
# coding: utf8

""" Action Recognition Model - Activity Classification from Videos.

    Loads a sample video and classifies using a trained model.

"""

import os

os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3'

import numpy as np
import tensorflow as tf
from airflow.operators.tfserving.base_classes.base_tf_model import BaseTFModel

from airflow.operators.tfserving.video.action_recognition.models.version1 import conf as model_config
from airflow.operators.tfserving.video.action_recognition.models.version1.action_recognition_model_helper import \
    ActionRecognitionModelHelper

# disable eager execution which is enabled by default in tf>=2.0
tf.compat.v1.disable_eager_execution()


class ActionRecognitionModel(BaseTFModel):
    """class to run inference for action recognition model"""

    _MODEL = model_config.MODEL_NAME
    _VERSION = model_config.MODEL_VERSION

    _FRAME_SIZE = model_config.FRAME_SIZE
    _HOP_SIZE = model_config.HOP_SIZE

    action_recognition_classmap = ActionRecognitionModelHelper().load_action_recognition_classmap()

    def __init__(self):
        """make a tf serving client."""
        super().__init__(self._MODEL, self._VERSION)

    def pre_process(self, input_data):
        rgb_sample = np.load(input_data)
        rgb_sample = (rgb_sample / 255.) * 2 - 1
        req_data = {self._TENSOR_NAME: 'input', self._TENSOR_DTYPE: tf.float32,
                    self._INPUT_DATA: np.array(rgb_sample)}
        return req_data

    def post_process(self, input_data, predictions):
        predictions = np.array(predictions['probabilities'][0])

        predictions_map = dict()
        for index in range(len(predictions)):
            predictions_map[self.action_recognition_classmap[index]] = predictions[index]

        return {input_data: predictions_map}
