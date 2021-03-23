#!/usr/bin/env python
# coding: utf8

""" Object Detection Model - Predict objects from images.

    Loads a sample image and obtains objects using a trained model.

"""

import os
import cv2
import numpy as np

os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3'

import tensorflow as tf
from airflow.operators.tfserving.base_classes.base_tf_model import BaseTFModel
from airflow.operators.tfserving.image.object_detection.models.object_detection_model.conf import object_models
from airflow.operators.tfserving.image.object_detection.models.object_detection_model.object_detection_model_helper \
    import ObjectDetectionModelHelper

# disable eager execution which is enabled by default in tf>=2.0
tf.compat.v1.disable_eager_execution()


class ObjectDetectionModel(BaseTFModel):
    """class to run inference for object detection model"""

    def __init__(self, model_name):
        """make a tf serving client."""
        self._model_config = object_models[model_name]
        self._MODEL = self._model_config['model_name']
        self._VERSION = self._model_config['version']
        self.label_map = ObjectDetectionModelHelper().load_object_detection_classmap()
        super().__init__(self._MODEL, self._VERSION)
        return

    def pre_process(self, input_data):
        image = cv2.imread(input_data)
        resized_image = cv2.resize(image, (self._model_config['image_width'], self._model_config['image_height']))
        resized_image = np.expand_dims(resized_image, axis=0)
        req_data = {
            self._TENSOR_NAME: self._model_config['tensor_name'],
            self._TENSOR_DTYPE: self._model_config['tensor_type'],
            self._INPUT_DATA: resized_image
        }
        return req_data

    def post_process(self, input_data, predictions):
        detection_classes, detection_scores, detection_boxes = predictions['detection_classes'][0], \
                                                               predictions['detection_scores'][0], \
                                                               predictions['detection_boxes'][0]
        detection_classes = [self.label_map.get(str(int(i)), 'unknown_key') for i in detection_classes]
        return detection_classes, detection_scores, detection_boxes
