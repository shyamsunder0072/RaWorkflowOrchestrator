from airflow.operators.tfserving.ioutils.argument_error import ArgumentError


class RangeValidator(object):
    def __init__(self, input_value, min_max):
        self.input_value = input_value
        self.min_value = min_max[0]
        self.max_value = min_max[1]

    def validate(self):
        if self.min_value <= self.input_value <= self.max_value:
            # print("passed range validator")
            pass
        else:
            # print("failed range validator")
            error_message = "OUT OF RANGE: provided value {} is outside the allowed range, " \
                            "provide a feasible value between {} and {}.".format(self.input_value, self.min_value, self.max_value)
            # print(message)
            raise ArgumentError(error_message)
