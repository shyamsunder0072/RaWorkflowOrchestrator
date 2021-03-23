import tensorflow as tf
from pkg_resources import resource_filename, Requirement

# classmap files
IMAGE_CLASSIFICATION_CLASSMAP = resource_filename(Requirement.parse("apache-airflow==1.0rc0"),
                                                  "airflow/operators/tfserving/data/imagenet_classes.txt")

classification_models = {
    'efficientnet_b4': {
        'model_name': 'efficientnet_b4',
        'version': 1,
        'tensor_name': 'image_input',
        'out_tensor_name': 'probs',
        'tensor_type': tf.float32,
        'batch_size': 1,
        'image_height': 380,
        'image_width': 380,
        'num_classes': 1000
    },
    'inception_v3': {
        'model_name': 'inception_v3',
        'version': 1,
        'tensor_name': 'inputs',
        'out_tensor_name': 'output_0',
        'tensor_type': tf.float32,
        'batch_size': 1,
        'image_height': 299,
        'image_width': 299,
        'num_classes': 1001
    },
    'mobilenet_v2': {
        'model_name': 'mobilenet_v2',
        'version': 1,
        'tensor_name': 'inputs',
        'out_tensor_name': 'output_0',
        'tensor_type': tf.float32,
        'batch_size': 1,
        'image_height': 224,
        'image_width': 224,
        'num_classes': 1001
    },
    'resnet_50': {
        'model_name': 'resnet_50',
        'version': 1,
        'tensor_name': 'input_1',
        'out_tensor_name': 'activation_49',
        'tensor_type': tf.float32,
        'batch_size': 1,
        'image_height': 224,
        'image_width': 224,
        'num_classes': 1001
    },
    'resnet_v2_152': {
        'model_name': 'resnet_v2_152',
        'version': 1,
        'tensor_name': 'inputs',
        'out_tensor_name': 'output_0',
        'tensor_type': tf.float32,
        'batch_size': 1,
        'image_height': 224,
        'image_width': 224,
        'num_classes': 1001
    }
}

