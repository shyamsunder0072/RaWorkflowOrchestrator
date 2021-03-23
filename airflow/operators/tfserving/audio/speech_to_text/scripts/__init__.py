# expose methods of this module
from airflow.operators.tfserving.audio.speech_to_text.scripts import stt_module

_script_helper_obj = stt_module.STTModule()
run_speech_to_text = _script_helper_obj.run_speech_to_text

