import os
import subprocess

"""IO related to directories. """


def check_model_exists(model_name, model_path):
    """helper function to check if model exists,
        provided the model path and an identifier to report its name.
    """

    assert os.path.exists(model_path), "{} does not exist at : {}".format(
        model_name, model_path
    )
    if os.path.exists(model_path):
        print("Found {} at : {}".format(
            model_name, model_path
        ))


def call_process(cmd):
    """helper function to run a bash process,
        provided the full command with all the arguments required.
    """

    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    o, e = proc.communicate()
    code = str(proc.returncode)
    return o, e, code


def create_directory(dir_):
    """create a directory if doesn't exists"""
    if not os.path.exists(dir_):
        os.makedirs(dir_)

