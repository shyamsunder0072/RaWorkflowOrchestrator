#!/usr/bin/env python
# coding: utf8

import os
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3'

import tensorflow as tf
from airflow.operators.tfserving.base_classes.base_tf_model import BaseTFModel
from airflow.operators.tfserving.image.face_key_points_detection.models.face_key_points_detection_model.conf \
    import models

# disable eager execution which is enabled by default in tf>=2.0
tf.compat.v1.disable_eager_execution()


class FaceKeyPointsDetectionModel(BaseTFModel):
    """class to run inference for face key points detection model"""

    def __init__(self, model_name):
        """make a tf serving client."""
        self._model_config = models[model_name]
        self._MODEL = self._model_config['model_name']
        self._VERSION = self._model_config['version']
        super().__init__(self._MODEL, self._VERSION)
        return

    def pre_process(self, input_data):
        req_data = {
            self._TENSOR_NAME: self._model_config['tensor_name'],
            self._TENSOR_DTYPE: self._model_config['tensor_type'],
            self._INPUT_DATA: input_data
        }
        return req_data

    def post_process(self, input_data, predictions):
        output = []
        for key in self._model_config['output_keys']:
            output.append(predictions[key])
        return output
