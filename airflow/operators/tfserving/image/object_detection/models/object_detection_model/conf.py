import tensorflow as tf
from pkg_resources import resource_filename, Requirement

# classmap files
OBJECT_DETECTION_CLASSMAP = resource_filename(Requirement.parse("apache-airflow==1.0rc0"),
                                              "airflow/operators/tfserving/data/coco_labelmap.json")

object_models = {
    'centernet': {
        'model_name': 'centernet',
        'version': 1,
        'tensor_name': 'input_tensor',
        'tensor_type': tf.uint8,
        'image_height': 512,
        'image_width': 512
    },
    'efficientdet': {
        'model_name': 'efficientdet',
        'version': 1,
        'tensor_name': 'input_tensor',
        'tensor_type': tf.uint8,
        'image_height': 640,
        'image_width': 640
    },
    'faster_rcnn': {
        'model_name': 'faster_rcnn',
        'version': 1,
        'tensor_name': 'input_tensor',
        'tensor_type': tf.uint8,
        'image_height': 640,
        'image_width': 640
    }
    ,
    'retinanet': {
        'model_name': 'retinanet',
        'version': 1,
        'tensor_name': 'input_tensor',
        'tensor_type': tf.uint8,
        'image_height': 640,
        'image_width': 640
    }
    ,
    'ssd_mobilenet': {
        'model_name': 'ssd_mobilenet',
        'version': 1,
        'tensor_name': 'input_tensor',
        'tensor_type': tf.uint8,
        'image_height': 640,
        'image_width': 640
    } 
}

