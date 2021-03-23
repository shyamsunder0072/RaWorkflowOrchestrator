from airflow.operators.tfserving.ioutils.argument_error import ArgumentError


class ChoiceValidator(object):
    def __init__(self, input_value, choices):
        self.input_value = input_value
        self.choices = choices

    def validate(self, error_message):
        val_flag = True
        try:
            self.choices.index(self.input_value)
        except ValueError:
            val_flag = False
        if val_flag:
            print("passed choice validator")
            pass
        else:
            print("failed choice validator")
            # message = "Set (Input/Default) argument value {} doesn't belong to the choice list. " \
            #           "Available choices are {}.".format("'" + self.input_value + "'", self.choices)
            # print(message)
            raise ArgumentError(error_message)

