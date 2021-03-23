import numpy as np
import resampy
import soundfile as sf
import tensorflow as tf
from airflow.operators.tfserving.audio.audio_classification.models.version1 import conf as model_config
from airflow.operators.tfserving.audio.audio_classification.models.version1.audio_classification_model_helper \
    import AudioClassificationModelHelper
from airflow.operators.tfserving.base_classes.base_tf_model import BaseTFModel


class AudioClassificationModel(BaseTFModel):
    _MODEL = model_config.MODEL_NAME
    _VERSION = model_config.MODEL_VERSION

    audio_classification_classmap = AudioClassificationModelHelper().load_audio_classification_classmap()

    def __init__(self):
        super().__init__(self._MODEL, self._VERSION)

    def pre_process(self, input_data):
        samples, sample_rate = sf.read(input_data, dtype=np.int16)
        assert samples.dtype == np.int16, 'Bad sample type: %r' % samples.dtype
        waveform = samples / 32768.0  # Convert to [-1.0, +1.0]
        # Convert to mono and the sample rate expected by YAMNet.
        if len(waveform.shape) > 1:
            waveform = np.mean(waveform, axis=1)
        if sample_rate != 16000:
            waveform = resampy.resample(waveform, sample_rate, 16000)
        req_data = {self._TENSOR_NAME: 'input_1', self._TENSOR_DTYPE: tf.float32,
                    self._INPUT_DATA: np.reshape(waveform, [1, -1])}
        return req_data

    def post_process(self, input_data, predictions):
        scores = predictions['predictions']
        prediction = np.mean(scores, axis=0)
        sorted_prediction = np.argsort(prediction)[::-1]
        predicted_sounds = {
            self.audio_classification_classmap[i]: round(prediction[i], 2)
            for i in sorted_prediction
            if round(prediction[i], 2) > 0.0
        }
        return {input_data: predicted_sounds}
