import multiprocessing

# tf serving model information
MODEL_NAME = "source_separation"
MODEL_VERSION = 1
SAMPLE_RATE = 16000  # 44100
OFFSET = 0
DURATION = 600.
BIT_RATE = '128k'
CODEC = 'wav'
FILE_FORMAT = '{filename}/{instrument}.{codec}'

# cores
total_cores = multiprocessing.cpu_count()
cores_per_inner_task = 2
max_concurrent_task = 3  # This pipeline runs at most 3 concurrent tasks
cores_per_task = max(1, total_cores//(max_concurrent_task*cores_per_inner_task))
