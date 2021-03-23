from airflow.operators.tfserving.ioutils.argument_category import ArgumentCategory
from airflow.operators.tfserving.ioutils.range_validator import RangeValidator


class ArgumentValidator(object):

    def __init__(self):
        return

    @staticmethod
    def validate_argument(func):
        def inner_validator(*args, **kwargs):
            arg_value = func(*args, **kwargs)
            # if arg_value is not None:
            # print(kwargs["category"])
            # print(arg_value)
            # print("validator decorator reached!")

            #  TODO: Uncomment category wise argument validators handling all kinds of edge cases,
            #   Ensure directory and file validators work for HDFS, S3 and object based file systems

            # if kwargs["category"] == ArgumentCategory.INPUT_DIRECTORY:
            #     # print("directory argument received")
            #     # error_message = "Set (Input/Default) directory {} doesn't exist.".format("'" + arg_value + "'")
            #     InputDirectoryValidator(arg_value).validate()
            # if kwargs["category"] == ArgumentCategory.OUTPUT_DIRECTORY:
            #     # print("directory argument received")
            #     # error_message = "Set (Input/Default) directory {} doesn't exist.".format("'" + arg_value + "'")
            #     OutputDirectoryValidator(arg_value).validate()
            # elif kwargs["category"] == ArgumentCategory.INPUT_FILE_PATH:
            #     # print("file argument received")
            #     # error_message = "Set (Input/Default) file {} doesn't exist.".format("'" + arg_value + "'")
            #     InputFileValidator(arg_value).validate()
            # # elif kwargs["category"] == ArgumentCategory.OUTPUT_FILE_PATH:
            #     # print("file argument received")
            #     # error_message = "Set (Input/Default) file {} doesn't exist.".format("'" + arg_value + "'")
            #     # OutputFileValidator(arg_value).validate()
            if kwargs["category"] in (ArgumentCategory.INTEGER_RANGE, ArgumentCategory.FLOAT_RANGE):
                # print("range argument received")
                # error_message = "Set (Input/Default) argument value {} is outside the allowed range. " \
                #                 "Provide a value between {} and {}.". format(arg_value, kwargs["range"])
                RangeValidator(arg_value, kwargs["min_max"]).validate()
            else:
                pass
            return arg_value

        return inner_validator
