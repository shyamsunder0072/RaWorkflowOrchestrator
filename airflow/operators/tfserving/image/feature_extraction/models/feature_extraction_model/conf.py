import tensorflow as tf

feature_extraction_models = {
    'efficientnet_b4': {
        'model_name': 'fv_efficientnet_b4',
        'version': 1,
        'tensor_name': 'image_input',
        'out_tensor_name': 'top_pool',
        'tensor_type': tf.float32,
        'batch_size': 1,
        'image_height': 380,
        'image_width': 380,
        'out_dims': 1792
    },
    'inception_v3': {
        'model_name': 'fv_inception_v3',
        'version': 2,
        'tensor_name': 'inputs',
        'out_tensor_name': 'feats',
        'tensor_type': tf.float32,
        'batch_size': 1,
        'image_height': 299,
        'image_width': 299,
        'out_dim': 2048
    },
    'mobilenet_v2': {
        'model_name': 'fv_mobilenet_v2',
        'version': 2,
        'tensor_name': 'inputs',
        'out_tensor_name': 'feats',
        'tensor_type': tf.float32,
        'batch_size': 1,
        'image_height': 224,
        'image_width': 224,
        'out_dim': 1208
    },
    'resnet_50': {
        'model_name': 'fv_resnet_50',
        'version': 1,
        'tensor_name': 'input_1',
        'out_tensor_name': 'reduce_mean',
        'tensor_type': tf.float32,
        'batch_size': 1,
        'image_height': 224,
        'image_width': 224,
        'out_dim': 2048
    },
    'resnet_v2_152': {
        'model_name': 'fv_resnet_v2_152',
        'version': 2,
        'tensor_name': 'inputs',
        'out_tensor_name': 'feats',
        'tensor_type': tf.float32,
        'batch_size': 1,
        'image_height': 224,
        'image_width': 224,
        'out_dim': 2048
    }
}
