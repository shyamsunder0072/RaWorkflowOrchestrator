# expose methods of this module
from airflow.operators.tfserving.video.action_recognition.scripts import action_module

_script_helper_obj = action_module.ActionModule()
run_action_recognition = _script_helper_obj.run_action_recognition

from airflow.operators.tfserving.video.action_recognition.models.version1.trie_builder import TrieBuilder
get_heirarchical_classes = TrieBuilder('').get_heirarchical_classes