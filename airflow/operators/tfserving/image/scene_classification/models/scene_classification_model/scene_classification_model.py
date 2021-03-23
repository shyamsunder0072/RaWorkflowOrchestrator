import io
import numpy as np
from PIL import Image
from airflow.operators.tfserving.base_classes.base_torch_model import BasePyTorchModel
from airflow.operators.tfserving.image.scene_classification.models.scene_classification_model.conf import image_models as class_config


class SceneClassificationModel(BasePyTorchModel):

    def __init__(self, model_name):
        self._model_config = class_config[model_name]
        self._MODEL = self._model_config['model_name']
        self._VERSION = self._model_config['version']
        super().__init__(self._MODEL, self._VERSION)
        return

    def pre_process(self, input_data):
        files = {
            'data': (input_data, open(input_data, 'rb')),
        }
        return files

    def post_process(self, input_data, predictions):
        return predictions
