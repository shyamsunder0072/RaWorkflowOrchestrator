import multiprocessing

from enum import Enum

# base helper defaults

try:
    import requests
    from airflow.models import Variable
    from hdfs import InsecureClient
    from hdfs.ext.kerberos import KerberosClient


    def get_hdfs_client():
        is_kerberized = Variable.get(key='IsHDFSSecured', default_var='False')
        hdfs_request_url = Variable.get('HDFSWebAddress')
        if is_kerberized == 'True':
            session = requests.Session()
            session.verify = False
            client = KerberosClient(hdfs_request_url, mutual_auth="REQUIRED", session=session)
        else:
            hdfs_user = Variable.get(key='HDFSUser', default_var=None)
            client = InsecureClient(hdfs_request_url, user=hdfs_user)

        return client

    hdfs_client = get_hdfs_client()
except:
    hdfs_client = ""


class ComputeStrategy(Enum):
    PARALLEL = "parallel"
    SEQUENTIAL = "sequential"


class FramesExtractionAlgo(Enum):
    FPS = "compute-fps"
    HIST = "compute-histogram"


class METRICS(Enum):
    DOT = "dot"
    ANGULAR = "angular"
    HAMMING = "hamming"
    MANHATTAN = "manhattan"
    EUCLIDEAN = "euclidean"


class TFServingModels(Enum):
    SPEECH_TO_TEXT = "SPEECH_TO_TEXT"
    SOURCE_SEPARATION = "SOURCE_SEPARATION"
    ACTION_RECOGNITION = "ACTION_RECOGNITION"
    AUDIO_CLASSIFICATION = "AUDIO_CLASSIFICATION"
    TEXT_BOXES_DETECTION = "TEXT_BOXES_DETECTION"
    SEMANTIC_SEGMENTATION = "SEMANTIC_SEGMENTATION"
    TEXT_EMBEDDINGS_EXTRACTION = "TEXT_EMBEDDINGS_EXTRACTION"
    AUDIO_EMBEDDINGS_EXTRACTION = "AUDIO_EMBEDDINGS_EXTRACTION"
    OPTICAL_CHARACTER_RECOGNITION = "OPTICAL_CHARACTER_RECOGNITION"
    OBJECT_DETECTION = "OBJECT_DETECTION"
    IMAGE_CLASSIFICATION = "IMAGE_CLASSIFICATION"
    IMAGE_FEATURE_EXTRACTION = "IMAGE_FEATURE_EXTRACTION"
    FACE_KEY_POINTS_DETECTION = 'FACE_KEY_POINTS_DETECTION'
    COLLAB_FILTERING_RECOMMENDATION = "COLLAB_FILTERING_RECOMMENDATION"
    FACE_EMBEDDINGS_EXTRACTION = "FACE_EMBEDDINGS_EXTRACTION"
    POSE_DETECTION = "POSE_DETECTION"


class PytorchServingModels(Enum):
    INSTANCE_SEGMENTATION = "INSTANCE_SEGMENTATION"
    SCENE_CLASSIFICATION = "SCENE_CLASSIFICATION"


class TFServingComputeFunctions(Enum):
    FRAME_TEXT_REDUCTION = "FRAME_TEXT_REDUCTION"
    AUDIO_SCENE_DETECTION = "AUDIO_SCENE_DETECTION"
    VIDEO_SCENE_DETECTION = "VIDEO_SCENE_DETECTION"
    RGB_STREAMS_COMPUTATION = "RGB_STREAMS_COMPUTATION"
    VIDEO_SYNOPSIS_GENERATOR = "VIDEO_SYNOPSIS_GENERATOR"
    KEY_FRAMES_EXTRACTION_FPS = "KEY_FRAMES_EXTRACTION_FPS"
    KEY_FRAMES_EXTRACTION_HIST = "KEY_FRAMES_EXTRACTION_HIST"
    COLOR_HISTOGRAM_COMPUTATION = "COLOR_HISTOGRAM_COMPUTATION"


# default task and inner task strategy
strategy_parallel = ComputeStrategy.PARALLEL.value
strategy_sequential = ComputeStrategy.SEQUENTIAL.value
default_inner_compute_strategy = strategy_parallel
default_compute_strategy = ComputeStrategy.PARALLEL

# distribution of cores among task
total_cores = multiprocessing.cpu_count()
tasks_per_child = 1000
num_concurrent_task = 2
num_total_cores_per_task = total_cores // num_concurrent_task
one_tier_compute_cores = max(2, num_total_cores_per_task // 1)
two_tier_compute_cores = max(1, num_total_cores_per_task // 2)
three_tier_compute_cores = max(1, num_total_cores_per_task // 3)

max_similar_items = 10

minimum_scene_duration = 1.0
maximum_scene_duration = -1
video_scene_detection_algorithm = 'detect-content'

default_color_histogram_type = 'hsv'
default_color_histogram_n_bins = 16

default_frame_rate = 1

num_trees = 100
metric = 'angular'

action_use_hierarchy = True

top_k = 10
top_k_sounds = top_k
top_k_objects = top_k
top_k_actions = top_k

sound_conf_threshold = 0.02
object_conf_threshold = 0.05
action_conf_threshold = 0.20

audio_sample_rate = "16000"

media_type = {
    'audio': ['.wav', '.mp3'],
    'video': ['.mp4'],
    'image': ['.jpeg', '.jpg', '.png'],
    'flat_file': ['.csv', '.parquet'],
    'serialized_file': ['.tfrecords'],
    'rgb_stream': ['.npy']
}
