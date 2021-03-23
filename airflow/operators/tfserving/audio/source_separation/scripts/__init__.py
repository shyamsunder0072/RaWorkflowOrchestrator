# expose methods of this module
from airflow.operators.tfserving.audio.source_separation.scripts import source_separation_module

_script_helper_obj = source_separation_module.SourceSeparationModule()
run_source_separation = _script_helper_obj.run_source_separation
