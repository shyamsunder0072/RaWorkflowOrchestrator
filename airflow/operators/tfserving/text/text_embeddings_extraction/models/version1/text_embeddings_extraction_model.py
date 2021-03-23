import tensorflow as tf

from airflow.operators.tfserving.base_classes.base_tf_model import BaseTFModel
from airflow.operators.tfserving.text.text_embeddings_extraction.models.version1 import conf as model_config


class TextEmbeddingsExtractionModel(BaseTFModel):

    _MODEL = model_config.MODEL_NAME
    _VERSION = model_config.MODEL_VERSION

    def __init__(self):
        super().__init__(self._MODEL, self._VERSION)

    def pre_process(self, input_data):
        req_data = {self._TENSOR_NAME: 'inputs', self._TENSOR_DTYPE: tf.string, self._INPUT_DATA: input_data}
        return req_data

    def post_process(self, input_data, predictions):
        computed_embeddings = predictions['outputs']
        return computed_embeddings
