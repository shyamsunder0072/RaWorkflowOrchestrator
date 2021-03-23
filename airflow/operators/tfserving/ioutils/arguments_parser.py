import json
import os

# from airflow.operators.tfserving.ioutils.directory_validator import InputDirectoryValidator, OutputDirectoryValidator
# from airflow.operators.tfserving.ioutils.file_validator import InputFileValidator, OutputFileValidator


class ArgumentsParser(object):

    def __init__(self):
        return

    """ parses arguments passed by TF Operator as json object. """
    @staticmethod
    def parse_arguments(args):
        args = json.loads(args)

        # TODO: check for corrupt inputs; file dir path uri format checks and raise error, design custom error messages
        try:
            """ validate input directory to check if it exists. """
            input_dir = args["input_base_dir_path"]
            # InputDirectoryValidator(input_dir).validate()
        except KeyError:
            input_dir = ""

        try:
            """ validate output directory to check if it exists or allowed to write to. """
            output_dir = args["output_base_dir_path"]
            # OutputDirectoryValidator(output_dir).validate()
        except KeyError:
            output_dir = ""

        try:
            """ validate file paths/names to check if it's a valid path. """
            input_files = args["input_filenames_dict"]
            for file in input_files.keys():
                # TODO: write file validator to support HDFS, S3 and object based file systems
                input_files[file] = os.path.join(input_dir, input_files[file])
                # InputFileValidator(input_files[file]).validate()
        except KeyError:
            input_files = dict()

        try:
            """ validate file paths/names to check if it's a valid path. """
            output_files = args["output_filenames_dict"]
            for file in output_files.keys():
                # OutputFileValidator(output_files[file]).validate()
                output_files[file] = os.path.join(output_dir, output_files[file])
        except KeyError:
            output_files = dict()
        try:
            task_args = args["method_args_dict"]
        except KeyError:
            task_args = dict()

        return input_dir, output_dir, input_files, output_files, task_args

    @staticmethod
    def parse_task(args):
        args = json.loads(args)
        try:
            task = args["method_id"]
        except KeyError:
            # TODO: log an error for missing task
            task = ""
        return task

    @staticmethod
    def parse_dag(args):
        args = json.loads(args)
        try:
            dag = args["dag_id"]
        except KeyError:
            dag = ""
        return dag
