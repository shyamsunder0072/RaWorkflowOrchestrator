#!/usr/bin/env python
# coding: utf8

""" Action Recognition - Activity Classification from Videos.

    Returns the top K action tags prediction using
    rgb streams of videos. It also makes improvements by post processing
    on model predictions.

"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from airflow.operators.tfserving.base_classes.base_module import BaseModule
from airflow.operators.tfserving.video.action_recognition.models.version1.action_recognition_model import \
    ActionRecognitionModel


class ActionModule(BaseModule):
    """class to return action predictions from a video. """

    action_recognition_model = ActionRecognitionModel()

    def __init__(self):
        super().__init__()

    def run_inference_per_input(self, rgb_stream):
        return self.action_recognition_model.run(rgb_stream)

    def run_action_recognition(self, rgb_stream_list, compute_cores=1, synchronous=True, **kwargs):
        """
            Get predictions from i3d 400 model.

        :param compute_cores:
        :param rgb_stream_list:
        :param synchronous:

        """
        return self.run_inference(rgb_stream_list, synchronous, compute_cores)
