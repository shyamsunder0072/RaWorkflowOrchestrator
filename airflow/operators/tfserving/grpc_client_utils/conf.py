# tf serving container host information
TF_SERVING_HOST = 'tfserving:8500'

# grpc predict stub parameter
TIMEOUT_PERIOD = 300.0  # in seconds

# grpc stub request parameters names
INPUT_DATA = 'data'
INPUT_TENSOR_NAME = 'in_tensor_name'
INPUT_TENSOR_DTYPE = 'in_tensor_dtype'

MAX_MESSAGE_LENGTH = 350000000
DEFAULT_SIGNATURE = 'serving_default'
MAX_RETRIES = 2

NUM_TO_DTYPE_VAL = {
    1: 'float_val',
    2: 'double_val',
    3: 'int_val',
    4: 'int_val',
    5: 'int_val',
    6: 'int_val',
    7: 'string_val',
    8: 'scomplex_val',
    9: 'int64_val',
    10: 'bool_val',
    18: 'dcomplex_val',
    19: 'half_val',
    20: 'resource_handle_val'
}
