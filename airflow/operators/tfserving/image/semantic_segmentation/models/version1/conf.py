from pkg_resources import resource_filename, Requirement

# classmap file
SEMANTIC_SEGMENTATION_CLASSMAP_FILE = resource_filename(Requirement.parse("apache-airflow==1.0rc0"), "airflow/operators/tfserving/data/semantic_segmentation_classmap.txt")

# tf serving model information
MODEL_NAME = "semantic_segmentation"
MODEL_VERSION = 1
INPUT_FRAME_SIZE = 513
TOTAL_FRAME_AREA = 288 * 513
