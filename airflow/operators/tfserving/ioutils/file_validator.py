import os

from airflow.operators.tfserving.ioutils.argument_error import ArgumentError
from pathvalidate import ValidationError, validate_filepath


class InputFileValidator(object):
    def __init__(self, file):
        self.file = file

    def validate(self):
        """validate if given input file exists or not"""
        if os.path.isfile(self.file) and os.path.exists(self.file):
            pass
        else:
            raise ArgumentError("[INPUT FILE] Non-Existent: provided file {} doesn't exists.".format(self.file))


class OutputFileValidator(object):
    def __init__(self, file):
        self.file = file

    def validate(self):
        """validate if given input qualifies as valid file path, raise error if blank, contains invalid characters or is a reserved name"""
        try:
            validate_filepath(self.file)
        except ValidationError as error:
            # print("failed file validator")
            # error = "[OUTPUT FILE]  Invalid file path:" + error
            raise ArgumentError(error)
        # else:
            # print("passed file validator")
