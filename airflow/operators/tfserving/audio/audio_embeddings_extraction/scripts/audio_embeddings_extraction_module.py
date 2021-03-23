#!/usr/bin/env python
# coding: utf8

""" Audio Embedding.

    Returns the embedding of audio files.

"""

from airflow.operators.tfserving.audio.audio_embeddings_extraction.models.version1.audio_embeddings_extraction_model import AudioEmbeddingsExtractionModel
from airflow.operators.tfserving.base_classes.base_module import BaseModule


class AudioEmbeddingsExtractionModule(BaseModule):
    """class to extract embedding from a audio. """

    audio_embeddings_extraction_model = AudioEmbeddingsExtractionModel()

    def __init__(self):
        super().__init__()

    def run_inference_per_input(self, audio):
        return self.audio_embeddings_extraction_model.run(audio)

    def run_audio_embeddings_extraction(self, audio_list, compute_cores=1, synchronous=False, **kwargs):
        """ Performs embedding extraction on audio files and returns the embeddings.
        :param compute_cores:
        :param audio_list: A list of audio files
        :param synchronous: (Optional) Default True for batching.
        """
        return self.run_inference(audio_list, synchronous, compute_cores)
