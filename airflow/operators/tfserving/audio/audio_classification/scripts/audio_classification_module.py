#!/usr/bin/env python
# coding: utf8

""" Audio Classification.

    Returns the top K sound tags prediction.

"""

from airflow.operators.tfserving.audio.audio_classification.models.version1.audio_classification_model import \
    AudioClassificationModel
from airflow.operators.tfserving.base_classes.base_module import BaseModule


class AudioClassificationModule(BaseModule):
    """class to return top k sound tags from a audio. """

    audio_classification_model = AudioClassificationModel()

    def __init__(self):
        super().__init__()

    def run_inference_per_input(self, audio):
        return self.audio_classification_model.run(audio)

    def run_audio_classification(self, segments_list, compute_cores=1, synchronous=False, **kwargs):
        """ Performs audio classification on audio segments and returns
        a list of sound tags with a confidence value.
        :param compute_cores:
        :param segments_list: A list of segments from a audio.
        :param synchronous: (Optional) Default True for batching.
        """
        return self.run_inference(segments_list, synchronous, compute_cores)
