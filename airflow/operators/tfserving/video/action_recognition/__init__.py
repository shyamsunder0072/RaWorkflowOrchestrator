# expose modules(scripts) of this package
from airflow.operators.tfserving.video.action_recognition.scripts import run_action_recognition
from airflow.operators.tfserving.video.action_recognition.models.version1.action_recognition_model_helper import \
    ActionRecognitionModelHelper

_model_helper_obj = ActionRecognitionModelHelper()
get_heirarchical_classes = _model_helper_obj.load_action_recognition_classes_tree()
