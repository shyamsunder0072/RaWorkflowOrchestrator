from airflow.operators.tfserving.video.video_synopsis_generation.scripts.video_synopsis_generation import \
    VideoSynopsisGeneration
run_video_synopsis_generation = VideoSynopsisGeneration().run_video_synopsis_generation
