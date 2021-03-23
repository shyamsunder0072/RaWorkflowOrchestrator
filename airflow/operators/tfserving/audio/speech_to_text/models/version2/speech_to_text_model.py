import os
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3'
import numpy as np
import tensorflow as tf
import tensorflow.compat.v1 as tfv1
from tensorflow.python.ops import gen_audio_ops as contrib_audio
from ds_ctcdecoder import ctc_beam_search_decoder, Scorer

from airflow.operators.tfserving.base_classes.base_tf_model import BaseTFModel
from airflow.operators.tfserving.audio.speech_to_text.models.version2.alphabet import Alphabet
from airflow.operators.tfserving.audio.speech_to_text.models.version2 import conf as model_config


"""Model class : Provides a utility function to initiate the graph of Speech To Text Model and to run inference 
to be used for Speech to text recognition  (Audio segments transcription).
"""


class SpeechToTextModel(BaseTFModel):
    """Class to load deeplab model and run inference."""

    _MODEL = model_config.MODEL_NAME
    _VERSION = model_config.MODEL_VERSION
    alphabet = Alphabet(model_config.alphabet_path)
    lm_alpha = model_config.lm_alpha
    lm_beta = model_config.lm_beta
    lm_binary_path = model_config.lm_binary_path
    lm_trie_path = model_config.lm_trie_path
    scorer = Scorer(lm_alpha, lm_beta, lm_binary_path, lm_trie_path, alphabet)

    def __init__(self):
        """Creates and loads pretrained speech to text model."""
        self.beam_width = model_config.beam_width
        self.cutoff_prob = model_config.cutoff_prob
        self.cutoff_top_n = model_config.cutoff_top_n
        self.n_context = model_config.n_context
        self.n_input = model_config.n_input
        self.audio_window_samples = model_config.audio_window_samples
        self.audio_step_samples = model_config.audio_step_samples
        self.n_cell_dim = model_config.n_cell_dim
        super().__init__(self._MODEL, self._VERSION)

    def create_overlapping_windows(self, batch_x):
        batch_size = tf.shape(input=batch_x)[0]
        window_width = 2 * self.n_context + 1
        num_channels = self.n_input
        eye_filter = tf.constant(np.eye(window_width * num_channels)
                                 .reshape(window_width, num_channels, window_width * num_channels), tf.float32)

        batch_x = tf.nn.conv1d(input=batch_x, filters=eye_filter, stride=1, padding='SAME')

        batch_x = tf.reshape(batch_x, [batch_size, -1, window_width, num_channels])

        return batch_x

    def samples_to_mfccs(self, samples, sample_rate):
        spectrogram = contrib_audio.audio_spectrogram(samples,
                                                      window_size=self.audio_window_samples,
                                                      stride=self.audio_step_samples,
                                                      magnitude_squared=True)

        mfccs = contrib_audio.mfcc(spectrogram, sample_rate, dct_coefficient_count=self.n_input)
        mfccs = tf.reshape(mfccs, [-1, self.n_input])

        return mfccs, tf.shape(input=mfccs)[0]

    def audiofile_to_features(self, wav_filename):
        samples = tf.io.read_file(wav_filename)
        decoded = contrib_audio.decode_wav(samples, desired_channels=1)
        features, features_len = self.samples_to_mfccs(decoded.audio, decoded.sample_rate)
        return features, features_len

    def create_input_data(self, audio):
        with tfv1.Session() as session:
            features, features_len = self.audiofile_to_features(audio)
            features = tf.expand_dims(features, 0)
            features_len = tf.expand_dims(features_len, 0)
            features = self.create_overlapping_windows(features).eval(session=session)
            features_len = features_len.eval(session=session)
            return features, features_len

    def pre_process(self, input_data):
        features, features_len = self.create_input_data(input_data)
        previous_state_c = np.zeros([1, self.n_cell_dim])
        previous_state_h = np.zeros([1, self.n_cell_dim])

        req_data = [
            {self._TENSOR_NAME: 'input', self._TENSOR_DTYPE: tf.float32, self._INPUT_DATA: features},
            {self._TENSOR_NAME: 'input_lengths', self._TENSOR_DTYPE: tf.int32, self._INPUT_DATA: features_len},
            {self._TENSOR_NAME: 'previous_state_c', self._TENSOR_DTYPE: tf.float32, self._INPUT_DATA: previous_state_c},
            {self._TENSOR_NAME: 'previous_state_h', self._TENSOR_DTYPE: tf.float32, self._INPUT_DATA: previous_state_h}
        ]
        return req_data

    def post_process(self, input_data, predictions):
        logits = predictions['outputs']
        logits = np.squeeze(logits)

        decoded = ctc_beam_search_decoder(logits, self.alphabet, self.beam_width, scorer=self.scorer,
                                          cutoff_prob=self.cutoff_prob, cutoff_top_n=self.cutoff_top_n)

        return {input_data: decoded[0][1]}
