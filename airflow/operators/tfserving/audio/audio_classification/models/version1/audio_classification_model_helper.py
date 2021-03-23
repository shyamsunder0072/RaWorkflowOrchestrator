import csv

import numpy as np
from airflow.operators.tfserving.audio.audio_classification.models.version1 import conf as model_config

"""Helper Class : Provides an utility function to load audio classification model and 
audioset classmap file. 
"""


class AudioClassificationModelHelper(object):
    """Helper class to load Audio Classification Model."""

    _CLASS_MAP_FILE = model_config.AUDIO_CLASSIFICATION_CLASS_MAP_FILE

    @staticmethod
    def get_classmap(class_map_csv):
        """Read the class name definition file and return a list of strings."""
        with open(class_map_csv) as csv_file:
            reader = csv.reader(csv_file)
            next(reader)  # Skip header
            return np.array([display_name for (_, _, display_name) in reader])

    # load class map
    def load_audio_classification_classmap(self):
        audio_classification_classmap = self.get_classmap(self._CLASS_MAP_FILE)
        return audio_classification_classmap

