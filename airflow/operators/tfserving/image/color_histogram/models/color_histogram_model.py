#!/usr/bin/env python
# coding: utf8

""" Color Histogram Model - Get color histogram features from images.

    Loads a sample image and obtains the color histogram.

"""

import cv2
import numpy as np

from airflow.operators.tfserving.base_classes.base_compute_model import BaseComputeModel


class ColorHistogramModel(BaseComputeModel):
    """class to run inference for color histogram model"""

    def __init__(self):
        return

    def pre_process(self, input_data):
        path, histogram_type, n_bins = input_data
        image = cv2.imread(path)
        if histogram_type == 'rgb':
            image = cv2.cvtColor(image, cv2.COLOR_BGR2RGB)
        elif histogram_type == 'hsv':
            image = cv2.cvtColor(image, cv2.COLOR_BGR2HSV)
        return image, n_bins

    def predict(self, model_input):
        img, n_bins = model_input
        hist = np.zeros((img.shape[2], n_bins))
        for i in range(img.shape[2]):
            hist[i] = cv2.calcHist([img], [i], None, [n_bins], [0, 256]).squeeze()
        hist = (hist - hist.min(axis=-1, keepdims=True)) / (
                    hist.max(axis=-1, keepdims=True) - hist.min(axis=-1, keepdims=True))
        c, h = hist.shape
        hist = np.reshape(hist, (c * h))
        return hist

    def post_process(self, input_data, predictions):
        return predictions
