# expose methods of this module
from airflow.operators.tfserving.image.semantic_segmentation.scripts import object_module

_script_helper_obj = object_module.ObjectModule()
run_semantic_segmentation = _script_helper_obj.run_semantic_segmentation

