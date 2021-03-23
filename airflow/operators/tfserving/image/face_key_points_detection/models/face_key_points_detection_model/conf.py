import tensorflow as tf

models = {
    'p_net': {
        'model_name': 'p_net',
        'version': 1,
        'tensor_name': 'input_1',
        'tensor_type': tf.float32,
        'output_keys': ['conv2d_4', 'softmax']
    },
    'r_net': {
        'model_name': 'r_net',
        'version': 1,
        'tensor_name': 'input_2',
        'tensor_type': tf.float32,
        'output_keys': ['dense_2', 'softmax_1']
    },
    'o_net': {
        'model_name': 'o_net',
        'version': 1,
        'tensor_name': 'input_3',
        'tensor_type': tf.float32,
        'output_keys': ['dense_5', 'dense_6', 'softmax_2']
    }
}
