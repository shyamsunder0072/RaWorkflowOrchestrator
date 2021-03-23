# expose methods of this module
from airflow.operators.tfserving.video.video_scene_detection.scripts import video_scene_detection_module

_script_helper_obj = video_scene_detection_module.VideoSceneDetectionModule()
run_video_scenes_detection = _script_helper_obj.run_video_scenes_detection

