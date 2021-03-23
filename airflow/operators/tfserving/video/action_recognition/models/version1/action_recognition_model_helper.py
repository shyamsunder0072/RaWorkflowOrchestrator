from airflow.operators.tfserving.video.action_recognition.models.version1 import conf as model_config
from airflow.operators.tfserving.video.action_recognition.models.version1.trie_builder import TrieBuilder as Trie

"""Helper Class : Provides an utility function to load action recognition model version 1 and its
respective classmap file.
"""


class ActionRecognitionModelHelper(object):
    """class to load action recognition model"""

    _ACTION_RECOGNITION_CLASSMAP_FILE = model_config.ACTION_RECOGNITION_CLASSMAP_FILE
    _ACTION_RECOGNITION_HIERARCHIES_FILE = model_config.ACTION_RECOGNITION_HIERARCHIES_FILE

    def load_action_recognition_classmap(self):
        action_recognition_classmap = [x.strip() for x in open(self._ACTION_RECOGNITION_CLASSMAP_FILE)]
        return action_recognition_classmap

    def load_action_recognition_classes_tree(self):
        tree = Trie('')
        with open(self._ACTION_RECOGNITION_HIERARCHIES_FILE, 'r') as f:
            file_data = f.read().split('\n')
            # First line contains column names
            for i in file_data[1:]:
                row = i.split(',')
                row = [j.strip() for j in row if len(j.strip()) >= 1]
                tree.insert(row[1:] + row[:1])
        return tree.get_heirarchical_classes

