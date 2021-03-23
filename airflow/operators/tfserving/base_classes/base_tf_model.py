#!/usr/bin/env python
# coding: utf8

import os

os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3'

import tensorflow as tf
from airflow.operators.tfserving.grpc_client_utils import ProdClient
from airflow.operators.tfserving.grpc_client_utils import conf as grpc_config


# disable eager execution which is enabled by default in tf>=2.0
tf.compat.v1.disable_eager_execution()


class BaseTFModel(object):

    _HOST = grpc_config.TF_SERVING_HOST

    _TENSOR_NAME = grpc_config.INPUT_TENSOR_NAME
    _TENSOR_DTYPE = grpc_config.INPUT_TENSOR_DTYPE
    _INPUT_DATA = grpc_config.INPUT_DATA

    def __init__(self, model, version):
        """make a tf serving client."""
        self._MODEL = model
        self._VERSION = version
        self.client = ProdClient(self._HOST, self._MODEL, self._VERSION)
        return

    def pre_process(self, input_data):
        raise NotImplementedError

    def predict(self, model_input):
        return self.client.predict(model_input)

    def post_process(self, input_data, predictions):
        raise NotImplementedError

    def run(self, input_data):
        """Runs inference on a numpy array.

        :param input_data: A numpy object, batch of data.

        """
        model_input = self.pre_process(input_data)
        model_response = self.predict(model_input)
        model_output = self.post_process(input_data, model_response)
        return model_output
