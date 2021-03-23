import tensorflow as tf

face_embedding_models = {
    'deepface': {
        'model_name': 'deepface',
        'version': 1,
        'tensor_name': 'C1_input',
        'out_tensor_name': 'F7',
        'tensor_type': tf.float32,
        'batch_size': 1,
        'image_height': 152,
        'image_width': 152
    },
    'facenet': {
        'model_name': 'facenet',
        'version': 1,
        'tensor_name': 'input_1',
        'out_tensor_name': 'Bottleneck_BatchNorm',
        'tensor_type': tf.float32,
        'batch_size': 1,
        'image_height': 160,
        'image_width': 160
    },
    'vgg_face': {
        'model_name': 'vgg_face',
        'version': 1,
        'tensor_name': 'zero_padding2d_13_input',
        'out_tensor_name': 'flatten_1',
        'tensor_type': tf.float32,
        'batch_size': 1,
        'image_height': 224,
        'image_width': 224
    }
}

