from enum import Enum, auto


class ArgumentCategory(Enum):
    STRING = auto()
    INTEGER = auto()
    FLOAT = auto()
    INPUT_DIRECTORY = auto()
    DIRECTORY_TREE_PATTERN = auto()
    OUTPUT_DIRECTORY = auto()
    INPUT_FILE_PATH = auto()
    OUTPUT_FILE_PATH = auto()
    ENUM = auto()
    BOOLEAN = auto()
    INTEGER_RANGE = auto()
    FLOAT_RANGE = auto()
    STRING_LIST = auto()
    INTEGER_LIST = auto()
    DATE = auto()
    DICT = auto()
