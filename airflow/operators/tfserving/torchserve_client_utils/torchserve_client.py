import requests


class PytorchClient:

    def __init__(self, host, model_name, model_version=None):
        self.host = host
        self.model_name = model_name
        self.model_version = model_version
        self.url = 'http://' + str(host) + '/predictions/' + str(self.model_name)

    def predict(self, data):
        response = requests.post(self.url, files=data)
        return response.content
