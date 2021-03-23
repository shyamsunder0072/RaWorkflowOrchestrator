import grpc
import numpy as np
import tensorflow as tf
from airflow.operators.tfserving.grpc_client_utils import conf as grpc_config
from tensorflow_serving.apis import predict_pb2
from tensorflow_serving.apis import prediction_service_pb2_grpc


class ProdClient:

    _TENSOR_NAME = grpc_config.INPUT_TENSOR_NAME
    _TENSOR_DTYPE = grpc_config.INPUT_TENSOR_DTYPE
    _INPUT_DATA = grpc_config.INPUT_DATA
    _DEFAULT_SIGNATURE = grpc_config.DEFAULT_SIGNATURE
    _NUM_TO_DTYPE_VAL = grpc_config.NUM_TO_DTYPE_VAL
    _MAX_MESSAGE_LENGTH = grpc_config.MAX_MESSAGE_LENGTH

    def __init__(self, host, model_name, model_version=None, max_retries=grpc_config.MAX_RETRIES):
        self.host = host
        self.model_name = model_name
        self.model_version = model_version
        self.max_tries = max_retries+1

    def predict(self, data):
        channel = grpc.insecure_channel(self.host,
                                        options=[
                                            ('grpc.max_send_message_length', self._MAX_MESSAGE_LENGTH),
                                            ('grpc.max_receive_message_length', self._MAX_MESSAGE_LENGTH),
                                        ])
        stub = prediction_service_pb2_grpc.PredictionServiceStub(channel)
        request = predict_pb2.PredictRequest()
        request.model_spec.name = self.model_name
        if self.model_version is not None:
            request.model_spec.version.value = self.model_version
        request.model_spec.signature_name = self._DEFAULT_SIGNATURE
        if type(data) == list:
            for d in data:
                request.inputs[d[self._TENSOR_NAME]].CopyFrom(
                    tf.compat.v1.make_tensor_proto(d[self._INPUT_DATA], d[self._TENSOR_DTYPE])
                )
        else:
            request.inputs[data[self._TENSOR_NAME]].CopyFrom(
                tf.compat.v1.make_tensor_proto(data[self._INPUT_DATA], data[self._TENSOR_DTYPE])
            )
        current_try = 0
        result = None
        while current_try < self.max_tries:
            current_try += 1
            try:
                result = stub.Predict(request, grpc_config.TIMEOUT_PERIOD)
                break
            except Exception as e:
                if current_try < self.max_tries:
                    continue
                raise e
        channel.close()
        return self.predict_response_to_dict(result)

    def predict_response_to_dict(self, predict_response):
        predict_response_dict = dict()

        for k in predict_response.outputs:
            shape = [x.size for x in predict_response.outputs[k].tensor_shape.dim]

            # logger.debug('Key: ' + k + ', shape: ' + str(shape))

            dtype_constant = predict_response.outputs[k].dtype

            if dtype_constant not in self._NUM_TO_DTYPE_VAL:
                # logger.error('Tensor output data type not supported. Returning empty dict.')
                predict_response_dict[k] = 'value not found'

            if shape == [1]:
                predict_response_dict[k] = \
                    eval('predict_response.outputs[k].' + self._NUM_TO_DTYPE_VAL[dtype_constant])[0]
            else:
                predict_response_dict[k] = np.array(
                    eval('predict_response.outputs[k].' + self._NUM_TO_DTYPE_VAL[dtype_constant])).reshape(shape)

        return predict_response_dict
