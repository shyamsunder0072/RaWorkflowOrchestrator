import pickle

from airflow.operators.tfserving.image.semantic_segmentation.models.version1 import conf as model_config

"""Helper class : Provides a utility function to load semantic segmentation model  and its labelmap
to be used for Semantic Segmentation (Video Scene Parsing).
"""


class SemanticSegmentationModelHelper(object):
    """class to load semantic segmentation model"""

    _CLASS_MAP_FILE = model_config.SEMANTIC_SEGMENTATION_CLASSMAP_FILE

    def load_semantic_segmentation_classmap(self):
        """Load semantic segmentation classmap file
        num_classes : 151
        """
        with open(self._CLASS_MAP_FILE, 'rb') as fp:
            semantic_segmentation_classmap = pickle.load(fp)
        return semantic_segmentation_classmap
