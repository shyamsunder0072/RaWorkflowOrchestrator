from airflow.operators.tfserving.ioutils.arguments_parser import ArgumentsParser


class BaseTask(object):
    """ base class to be extended by every task for standard code conventions. """

    def __init__(self):
        return

    def execute(self, args):
        arg_parser = ArgumentsParser()
        """ parse task arguments (json object) and make preliminary checks on directory and file inputs. """
        input_base_dir_path, output_base_dir_path, input_file_names_dict, output_file_names_dict, method_args_dict = \
            arg_parser.parse_arguments(args)
        self.set_params(input_base_dir_path, output_base_dir_path, input_file_names_dict,
                        output_file_names_dict, method_args_dict)
        self.run_task()

    @staticmethod
    def set_params(input_base_dir_path, output_base_dir_path, input_file_names_dict,
                   output_file_names_dict, method_args_dict):
        """ set task parameters from task arguments or pre-defined default values,
         raise argument warning/error if validation fails.
        set_params is overwritten by every task as per the requirement."""
        return

    @staticmethod
    def run_task():
        """ initiate task helper object with globally set task parameters by
         set_params method and execute relevant helper method. """
        return
