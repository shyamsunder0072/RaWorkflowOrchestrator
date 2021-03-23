# expose methods of this module
from airflow.operators.tfserving.audio.audio_scene_detection.scripts import audio_scene_detection_module

_script_helper_obj = audio_scene_detection_module.AudioSceneDetectionModule()
run_audio_scene_detection = _script_helper_obj.run_audio_scene_detection

