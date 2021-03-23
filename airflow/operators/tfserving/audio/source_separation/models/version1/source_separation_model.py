import tensorflow as tf

from airflow.operators.tfserving.audio.source_separation.models.version1 import conf as model_config
from airflow.operators.tfserving.base_classes.base_tf_model import BaseTFModel
from airflow.operators.tfserving.audio.source_separation.models.version1.utilities import get_default_audio_adapter, \
    to_stereo


class SourceSeparationModel(BaseTFModel):
    _MODEL = model_config.MODEL_NAME
    _VERSION = model_config.MODEL_VERSION

    audio_adapter = get_default_audio_adapter

    def __init__(self):
        super().__init__(self._MODEL, self._VERSION)
        self.sample_rate = model_config.SAMPLE_RATE
        self._offset = model_config.OFFSET
        self._duration = model_config.DURATION

    def pre_process(self, input_data):
        waveform, _ = self.audio_adapter.load(
            input_data,
            offset=self._offset,
            duration=self._duration,
            sample_rate=self.sample_rate)
        if not waveform.shape[-1] == 2:
            waveform = to_stereo(waveform)
        req_data = {self._TENSOR_NAME: 'waveform', self._TENSOR_DTYPE: tf.float32, self._INPUT_DATA: waveform}
        return req_data

    def post_process(self, input_data, predictions):
        return predictions
