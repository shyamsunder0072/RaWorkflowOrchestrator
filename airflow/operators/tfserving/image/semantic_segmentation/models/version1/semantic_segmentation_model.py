#!/usr/bin/env python
# coding: utf8

""" Semantic Segmentation Model - Video Scene Parsing.

    Provides a utility function to initiate the graph of Semantic Segmentation Model and to run inference
    to be used for Semantic Segmentation (Video Scene Parsing).

"""

import numpy as np
import tensorflow as tf
from PIL import Image

from airflow.operators.tfserving.base_classes.base_tf_model import BaseTFModel
from airflow.operators.tfserving.image.semantic_segmentation.models.version1 import conf as model_config
from airflow.operators.tfserving.image.semantic_segmentation.models.version1.semantic_segmentation_model_helper \
    import SemanticSegmentationModelHelper


class SemanticSegmentationModel(BaseTFModel):
    """Class to load deeplab model and run inference."""

    _MODEL = model_config.MODEL_NAME
    _VERSION = model_config.MODEL_VERSION

    _INPUT_SIZE = model_config.INPUT_FRAME_SIZE

    semantic_segmentation_classmap = SemanticSegmentationModelHelper().load_semantic_segmentation_classmap()

    def __init__(self):
        """Creates and loads pretrained semantic segmentation model."""
        self._TOTAL = model_config.TOTAL_FRAME_AREA
        super().__init__(self._MODEL, self._VERSION)

    def pre_process(self, input_data):
        image = Image.open(input_data)
        width, height = image.size
        resize_ratio = 1.0 * self._INPUT_SIZE / max(width, height)
        target_size = (int(resize_ratio * width), int(resize_ratio * height))
        resized_image = image.convert('RGB').resize(target_size, Image.ANTIALIAS)

        req_data = {self._TENSOR_NAME: 'input', self._TENSOR_DTYPE: tf.uint8,
                    self._INPUT_DATA: np.array([np.asarray(resized_image)])}
        return req_data

    def post_process(self, input_data, predictions):
        batch_seg_map = predictions['probabilities']
        seg_map = batch_seg_map[0]

        uniq_elements, counts = np.unique(seg_map, return_counts=True)
        zipped_predictions = zip(uniq_elements, counts)
        sorted_predictions = sorted(zipped_predictions, key=lambda x: x[1], reverse=True)
        predictions_map = {}

        for _, (lbl_ind, cnt) in enumerate(sorted_predictions):
            if (cnt / self._TOTAL) > 0.0:
                if self.semantic_segmentation_classmap[lbl_ind] in predictions_map.keys():
                    if predictions_map[self.semantic_segmentation_classmap[lbl_ind]] < (cnt / self._TOTAL):
                        predictions_map[self.semantic_segmentation_classmap[lbl_ind]] = cnt / self._TOTAL
                else:
                    predictions_map[self.semantic_segmentation_classmap[lbl_ind]] = cnt / self._TOTAL

        return {input_data: predictions_map}, {input_data: seg_map}
