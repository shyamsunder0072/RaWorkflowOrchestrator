from airflow.operators.tfserving.torchserve_client_utils import conf_torch
from airflow.operators.tfserving.torchserve_client_utils.torchserve_client import PytorchClient


class BasePyTorchModel(object):

    _HOST = conf_torch.TORCH_SERVING_HOST

    def __init__(self, model, version):

        """make a torch serving client."""
        self._MODEL = model
        self._VERSION = version
        self.client = PytorchClient(self._HOST, self._MODEL, self._VERSION)
        return

    def pre_process(self, input_data):
        raise NotImplementedError

    def predict(self, model_input):
        return self.client.predict(model_input)

    def post_process(self, input_data, predictions):
        raise NotImplementedError

    def run(self, input_data):
        model_input = self.pre_process(input_data)
        model_response = self.predict(model_input)
        model_output = self.post_process(input_data, model_response)
        return model_output
