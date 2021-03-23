from airflow.operators.tfserving.image.image_classification.models.image_classification_model \
    import conf as model_config

"""Helper Class : Provides an utility function to load Image Classification Model label map. 
"""


class ImageClassificationHelper(object):
    """Helper class to load Image Classification Model."""

    _CLASS_MAP_FILE = model_config.IMAGE_CLASSIFICATION_CLASSMAP

    @staticmethod
    def get_classmap(class_map_json):
        """Read the class name definition file and returns label mapping."""
        with open(class_map_json, 'r') as f:
            classes = [i.strip() for i in f.readlines()]
        return classes

    # load class map
    def load_image_classification_classmap(self, model_name):
        image_classes = self.get_classmap(self._CLASS_MAP_FILE)
        if model_config.classification_models[model_name]['num_classes'] != len(image_classes):
            # Remove Background class
            image_classes = image_classes[1:]
        return image_classes
