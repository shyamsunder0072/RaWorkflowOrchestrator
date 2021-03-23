import os

from airflow.operators.tfserving.ioutils.argument_error import ArgumentError


class InputDirectoryValidator(object):
    def __init__(self, directory):
        self.directory = directory

    def validate(self):
        """try accessing input directory's content, raise error if not found or not a directory."""
        try:
            os.listdir(self.directory)
        except (NotADirectoryError, FileNotFoundError) as error:
            raise ArgumentError(error)


class OutputDirectoryValidator(object):
    def __init__(self, directory):
        self.directory = directory

    def validate(self):
        """check if output directory exist, if it doesn't, create it and raise error if not allowed to do so."""
        try:
            if not os.path.exists(os.path.dirname(self.directory)):
                os.makedirs(os.path.dirname(self.directory))
            else:
                pass
        except (FileNotFoundError, PermissionError) as error:
            raise ArgumentError(error)


