import tensorflow as tf

models = {
    'posenet_resnet_16': {
        'model_name': 'posenet_resnet_16',
        'version': 1,
        'tensor_name': 'sub_2',
        'tensor_type': tf.float32,
        'output_tensor_mapping': {
            'heatmaps': 'float_heatmaps',
            'offsets': 'float_short_offsets',
            'displacement_backward': 'resnet_v1_50/displacement_bwd_2/BiasAdd',
            'displacement_forward': 'resnet_v1_50/displacement_fwd_2/BiasAdd'
        }
    },
    'posenet_resnet_32': {
        'model_name': 'posenet_resnet_32',
        'version': 1,
        'tensor_name': 'sub_2',
        'tensor_type': tf.float32,
        'output_tensor_mapping': {
            'heatmaps': 'float_heatmaps',
            'offsets': 'float_short_offsets',
            'displacement_backward': 'resnet_v1_50/displacement_bwd_2/BiasAdd',
            'displacement_forward': 'resnet_v1_50/displacement_fwd_2/BiasAdd'
        }
    },
    'posenet_mobilenet_8': {
        'model_name': 'posenet_mobilenet_8',
        'version': 1,
        'tensor_name': 'sub_2',
        'tensor_type': tf.float32,
        'output_tensor_mapping': {
            'heatmaps': 'MobilenetV1/heatmap_2/BiasAdd',
            'offsets': 'MobilenetV1/offset_2/BiasAdd',
            'displacement_backward': 'MobilenetV1/displacement_bwd_2/BiasAdd',
            'displacement_forward': 'MobilenetV1/displacement_fwd_2/BiasAdd'
        }
    },
    'posenet_mobilenet_16': {
        'model_name': 'posenet_mobilenet_16',
        'version': 1,
        'tensor_name': 'sub_2',
        'tensor_type': tf.float32,
        'output_tensor_mapping': {
            'heatmaps': 'MobilenetV1/heatmap_2/BiasAdd',
            'offsets': 'MobilenetV1/offset_2/BiasAdd',
            'displacement_backward': 'MobilenetV1/displacement_bwd_2/BiasAdd',
            'displacement_forward': 'MobilenetV1/displacement_fwd_2/BiasAdd'
        }
    },
}

