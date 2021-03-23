import json

from airflow.operators.tfserving.image.object_detection.models.object_detection_model import conf as model_config

"""Helper Class : Provides an utility function to load Object Detection Model label map. 
"""


class ObjectDetectionModelHelper(object):
    """Helper class to load Object Detection Model."""

    _CLASS_MAP_FILE = model_config.OBJECT_DETECTION_CLASSMAP

    @staticmethod
    def get_classmap(class_map_json):
        """Read the class name definition file and returns label mapping."""
        with open(class_map_json, 'r') as f:
            label_map = json.load(f)
        return label_map

    # load class map
    def load_object_detection_classmap(self):
        object_detection_classmap = self.get_classmap(self._CLASS_MAP_FILE)
        return object_detection_classmap

