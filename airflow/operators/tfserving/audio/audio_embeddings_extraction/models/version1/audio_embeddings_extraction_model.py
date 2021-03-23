import os

os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3'

import librosa
import tensorflow as tf

from airflow.operators.tfserving.base_classes.base_tf_model import BaseTFModel
from airflow.operators.tfserving.audio.audio_embeddings_extraction.models.version1 import conf as model_config

tf.compat.v1.disable_eager_execution()


class AudioEmbeddingsExtractionModel(BaseTFModel):

    _MODEL = model_config.MODEL_NAME
    _VERSION = model_config.MODEL_VERSION

    def __init__(self):
        self.sample_rate = model_config.sample_rate
        super().__init__(self._MODEL, self._VERSION)

    def pre_process(self, input_data):
        wav, sr = librosa.load(input_data, sr=self.sample_rate)
        req_data = [{
            self._TENSOR_NAME: 'waveform',
            self._TENSOR_DTYPE: tf.float32,
            self._INPUT_DATA: wav
        }]
        return req_data

    def post_process(self, input_data, predictions):
        embedding = predictions['embeddings'].mean(axis=0)
        return {input_data: embedding}
