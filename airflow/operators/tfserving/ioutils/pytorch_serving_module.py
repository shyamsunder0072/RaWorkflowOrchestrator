import sys
from importlib import import_module


def map_pytorchserving_model(model_name):
    root_package = "airflow.operators.tfserving."
    module_imports = {
        "INSTANCE_SEGMENTATION": [root_package + "image.instance_segmentation", "run_masking"],
        "SCENE_CLASSIFICATION" : [root_package+ "image.scene_classification", "run_scene_classification"]
    }
    models_list = list(module_imports.keys())
    if model_name in models_list:
        module_imports = module_imports.get(model_name)
    else:
        print("ModelNotFoundError: \'{}\' No such model found. \nList of models available : \n{}".format(
            model_name, models_list
        ))
        sys.exit(1)

    """get the method object."""
    return getattr(import_module(module_imports[0]), module_imports[1])
