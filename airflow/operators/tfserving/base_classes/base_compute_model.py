#!/usr/bin/env python
# coding: utf8


class BaseComputeModel(object):
    """A generic class to run inference for compute functions (i.e. non-TF, non-Pytorch functions)"""

    def __init__(self, **kwargs):
        self.kwargs = kwargs
        return

    def pre_process(self, input_data):
        raise NotImplementedError

    def predict(self, model_input):
        raise NotImplementedError

    def post_process(self, input_data, predictions):
        raise NotImplementedError

    def run(self, input_data):
        model_input = self.pre_process(input_data)
        model_response = self.predict(model_input)
        model_output = self.post_process(input_data, model_response)
        return model_output
