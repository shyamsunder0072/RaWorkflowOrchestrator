import io
import numpy as np
from PIL import Image

from airflow.operators.tfserving.base_classes.base_torch_model import BasePyTorchModel
from airflow.operators.tfserving.image.instance_segmentation.models.yolact import conf as model_config


class YolactNetModel(BasePyTorchModel):

    _MODEL = model_config.MODEL_NAME
    _VERSION = model_config.MODEL_VERSION

    def __init__(self):
        super().__init__(self._MODEL, self._VERSION)
        return

    def pre_process(self, input_data):
        files = {
            'data': (input_data, open(input_data, 'rb')),
        }
        return files

    def post_process(self, input_data, predictions):

        mask = Image.open(io.BytesIO(predictions))
        mask = np.array(mask)
        mask = mask.astype(float)
        return mask
