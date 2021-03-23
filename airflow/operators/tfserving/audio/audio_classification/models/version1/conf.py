from pkg_resources import resource_filename, Requirement

# classmap files
AUDIO_CLASSIFICATION_CLASS_MAP_FILE = resource_filename(Requirement.parse("apache-airflow==1.0rc0"), "airflow/operators/tfserving/data/audio_classification_classmap.csv")

# tf serving model information
MODEL_NAME = 'audio_classification'
MODEL_VERSION = 1
