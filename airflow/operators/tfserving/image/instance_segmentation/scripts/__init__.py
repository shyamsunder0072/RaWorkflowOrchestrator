from airflow.operators.tfserving.image.instance_segmentation.scripts.instance_segmentation_module import \
    InstanceSegmentationModule

_script_helper_obj = InstanceSegmentationModule()
run_masking = _script_helper_obj.run_masking
