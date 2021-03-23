import warnings
from dateutil.parser import *
from airflow.operators.tfserving.ioutils.argument_category import ArgumentCategory
from airflow.operators.tfserving.ioutils.argument_error import ArgumentError, ArgumentWarning
from airflow.operators.tfserving.ioutils.argument_validator import ArgumentValidator


class ArgumentGetter(object):
    def __init__(self):
        # , input_value, category, default_value):
        return
        # self.input_value = input_value
        # self.category: ArgumentCategory = category
        # self.default_value = default_value

    # def __setattr__(self, name, value):
    #     if name == 'category' and not isinstance(value, ArgumentCategory):
    #         raise TypeError('ArgumentSetter.category must be of type ArgumentCategory')
    #     super().__setattr__(name, value)

    argument_validator = ArgumentValidator().validate_argument

    @staticmethod
    @argument_validator
    def __try_argument(value, category, data_type, min_max, arg_name):
        """ sanitize argument value and type cast to required data type format.
            validate value using argument validator decorator.
            raise argument error/warning if value fails data type casting or validation checks.
        """
        garbage_values = [None, ""]
        # print(argument_value)
        if value not in garbage_values:
            try:
                if category == ArgumentCategory.STRING_LIST:
                    if isinstance(value, list):
                        argument_value = value
                        for x in argument_value:
                            if not isinstance(x, str):
                                raise ValueError
                    elif isinstance(value, str):
                        argument_value = eval(value)
                        if not isinstance(argument_value, list):
                            raise ValueError
                        for x in argument_value:
                            if not isinstance(x, str):
                                raise ValueError
                    else:
                        raise ValueError
                elif category == ArgumentCategory.INTEGER_LIST:
                    if isinstance(value, list):
                        argument_value = [int(x) for x in value]
                    elif isinstance(value, str):
                        argument_value = [int(x) for x in eval(value)]
                    else:
                        raise ValueError
                elif category == ArgumentCategory.DATE:
                    value = str(value)
                    parse(value, fuzzy=False)
                    argument_value = parse(value).strftime("%d%m%Y")
                elif category == ArgumentCategory.DICT:
                    if isinstance(value, dict):
                        argument_value = value
                    elif isinstance(value, str):
                        argument_value = eval(value)
                        if not isinstance(argument_value, dict):
                            raise ValueError
                else:
                    argument_value = data_type(value)
            except (ValueError, TypeError, SyntaxError, NameError) as error:
                msg = "INCORRECT TYPE: provided value {} for argument {} can't be converted to desired {} data type." \
                    .format(value, arg_name, data_type)
                warnings.warn(msg, ArgumentWarning)
                raise ArgumentError(str(error))
        else:
            msg = "INVALID VALUE: {} provided for argument {}.".format(value, arg_name)
            warnings.warn(msg, ArgumentWarning)
            raise ArgumentError(str(msg))
        return argument_value

    @staticmethod
    def __to_bool(value):
        """ converts 'something' to boolean, raises error for invalid formats.
            possible True  values: 1, True, "1", "TRUE", "yes", "y", "t", ...
            possible False values: 0, False, "0", "faLse", "no", "n", "f", 0.0, ...
        """

        if str(value).lower() in ("yes", "y", "true", "t", "1", "1.0"):
            return True
        if str(value).lower() in ("no", "n", "false", "f", "0", "0.0"):
            return False
        raise ArgumentError('Invalid value for boolean type: {}.'.format(str(value)))

    def get_argument(self, input_value, category: ArgumentCategory, **kwargs):
        # default_value=None, data_type: type = str,
        # min_max: tuple = (0, 1), arg_name: str = ""):
        """ auto infer data type from category value.
            try to set argument value using user provided input value, raise warning if input value fails any validation checks.
            try to set argument value using default argument value instead, raise error if default value fails any validation checks.
        """

        default_value = kwargs.get('default_value', None)
        data_type: type = kwargs.get('data_type', str)
        min_max: tuple = kwargs.get('min_max', (0, 1))
        arg_name: str = kwargs.get('arg_name', "")

        if category in (ArgumentCategory.INPUT_FILE_PATH, ArgumentCategory.OUTPUT_FILE_PATH):
            data_type = str
        elif category in (ArgumentCategory.INTEGER, ArgumentCategory.INTEGER_RANGE):
            data_type = int
        elif category in (ArgumentCategory.FLOAT, ArgumentCategory.FLOAT_RANGE):
            data_type = float
        elif category == ArgumentCategory.DICT:
            data_type = dict
        elif category == ArgumentCategory.ENUM:
            data_type = data_type
        elif category == ArgumentCategory.STRING_LIST:
            data_type = list

        elif category == ArgumentCategory.INTEGER_LIST:
            data_type = list

        elif category == ArgumentCategory.BOOLEAN:
            data_type = self.__to_bool
        else:
            data_type = data_type

        try:
            # print("try")
            # print(input_value)
            argument_value = self.__try_argument(value=input_value, category=category, data_type=data_type, min_max=min_max, arg_name=arg_name)
        except ArgumentError as error:
            msg = "INCORRECT VALUE: provided value {} for argument {} failed, trying default value {} instead." \
                .format(input_value, arg_name, default_value)
            warnings.warn(msg, ArgumentWarning)
            # print("except")
            # print(default_value)
            argument_value = self.__try_argument(value=default_value, category=category, data_type=data_type, min_max=min_max, arg_name=arg_name)
        return argument_value

    def get_argument_dict(self, arg_dict, key_name, category: ArgumentCategory, **kwargs):
        # default_value, data_type: type = str,
        # min_max: tuple = (0, 1), dict_name: str = ""):

        default_value = kwargs.get('default_value', None)
        data_type: type = kwargs.get('data_type', str)
        min_max: tuple = kwargs.get('min_max', (0, 1))
        dict_name: str = kwargs.get('arg_name', "")

        try:
            arg_dict[key_name]
        except KeyError:
            msg = "MISSING: {} key not found in argument dict {}.".format(key_name, dict_name)
            warnings.warn(msg, ArgumentWarning)

        argument_value = self.get_argument(input_value=arg_dict.get(key_name), category=category, default_value=default_value,
                                           data_type=data_type, min_max=min_max, arg_name=key_name + " in " + dict_name + " dict")
        return argument_value


"""
if __name__ == "__main__":
    processed_dir = "/Users/dimplebansal/Downloads/similar_videos/"
    output_dir = "/Users/dimplebansal/Downloads/similar_videos/"
    input_base_dir_path = processed_dir
    # output_dir
    # input_filenames_dict = {'embedding_file': "embeddings.tfrecords"}
    input_filenames_dict = {}
    output_base_dir_path = output_dir
    # processed_dir
    output_filenames_dict = {'index_file': "embed.index"}
    method_args_dict = {'embedding_vector_size': "512", 'metric': "angular", 'num_trees': "100", 'index': 0}

    application_args_dict = {}
    application_args_dict.update({'method_args_dict': method_args_dict})
    application_args_dict.update({'input_base_dir_path': input_base_dir_path})
    application_args_dict.update({'output_base_dir_path': output_base_dir_path})
    application_args_dict.update({'input_filenames_dict': input_filenames_dict})
    application_args_dict.update({'output_filenames_dict': output_filenames_dict})
    args = json.dumps(application_args_dict)

    ap = ArgumentsParser()
    input_base_dir_path, output_base_dir_path, input_file_names_dict, output_file_names_dict, method_args_dict = ap.parse_arguments(args)
    print(input_base_dir_path)
    print(output_base_dir_path)
    print(input_file_names_dict)
    print(output_filenames_dict)
    print(method_args_dict)

    asc = ArgumentGetter()
    # a = asc.get_argument(category=ArgumentCategory.INPUT_DIRECTORY, input_value=input_base_dir_path,
    #                      default_value="xyz/abc")
    # print(a)
    # a = asc.get_argument(category=ArgumentCategory.INPUT_FILE_PATH, input_value=input_file_names_dict.get('embedding_file'),
    #                      default_value="xyz.tfrecords", arg_name="emb_file")
    # print(a)

    l = ["k"]
    a = asc.get_argument(category=ArgumentCategory.ENUM, input_value=l[0],
                         default_value=METRICS.ANGULAR, data_type=METRICS, arg_name="index_metric")
    print(a)
    a = asc.get_argument(input_value="10000", category=ArgumentCategory.INTEGER_RANGE,
                         default_value=100.1, min_max=(1, 100), arg_name="index_trees")
    print(a)
    a = asc.get_argument(input_value="9", category=ArgumentCategory.BOOLEAN,
                         default_value=False, arg_name="boo")
    print(a)
    a = asc.get_argument(input_value="10000", category=ArgumentCategory.INTEGER_RANGE,
                         default_value="1", min_max=(1, 100))
    print(a)

    argument_getter = ArgumentSetter().get_argument_dict

    video_embedding_file = argument_getter(input_file_names_dict, 'embedding_file', ArgumentCategory.INPUT_FILE_PATH,
                                           default_value="/Users/dimplebansal/Downloads/similar_videos/embeddings.tfrecords")
                                           # dict_name="input_file_names_dict")

    print(video_embedding_file)
"""
