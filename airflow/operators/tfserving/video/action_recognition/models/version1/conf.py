from pkg_resources import resource_filename, Requirement

# classmap files
ACTION_RECOGNITION_CLASSMAP_FILE = resource_filename(Requirement.parse("apache-airflow==1.0rc0"), "airflow/operators/tfserving/data/action_recognition_classmap.txt")
ACTION_RECOGNITION_HIERARCHIES_FILE = resource_filename(Requirement.parse("apache-airflow==1.0rc0"), "airflow/operators/tfserving/data/action_recognition_hierarchies.csv")

# tf serving model information
MODEL_NAME = "action_recognition"
MODEL_VERSION = 1
NUM_CLASSES = 400
FRAME_SIZE = 250
HOP_SIZE = 125
