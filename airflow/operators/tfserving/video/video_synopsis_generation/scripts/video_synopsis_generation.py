import os

from airflow.operators.tfserving.base_classes.base_module import BaseModule
from airflow.operators.tfserving.ioutils.utilities import get_video_length


class VideoSynopsisGeneration(BaseModule):
    """class to generate synopsis from a video. """

    def __init__(self):
        super().__init__()

    @staticmethod
    def split_video(video_path, start_time, out_path, duration=None):
        if duration:
            cmd = 'ffmpeg -nostats -loglevel error -i "{}" -ss {} -t {} -y "{}"'.format(
                video_path, int(start_time), int(duration), out_path)
        else:
            cmd = 'ffmpeg -nostats -loglevel error -i "{}" -ss {} -y "{}"'.format(
                video_path, int(start_time), out_path)
        os.system(cmd)

    @staticmethod
    def concat_videos(video_path1, video_path2, out_path):
        #  Create a file containing video ids
        ext = os.path.splitext(os.path.basename(out_path))[1]
        temp_file = out_path.replace(ext, '.txt')
        with open(temp_file, 'w') as f:
            f.write("file '{}'\n".format(video_path1))
            f.write("file '{}'\n".format(video_path2))
        cmd = 'ffmpeg -nostats -loglevel error -f concat -safe 0 -i "{}" -c copy -y "{}"'.format(temp_file, out_path)
        os.system(cmd)
        os.remove(video_path1)
        os.remove(video_path2)
        os.remove(temp_file)

    def run_inference_per_input(self, video_path, max_duration_threshold):
        max_duration_threshold_secs = (max_duration_threshold//2) * 60
        video_length = get_video_length(video_path)
        if video_length > (2 * max_duration_threshold_secs):
            print("[Info] Processing {} : {}".format(video_path, video_length))
            basename = os.path.splitext(video_path)
            out_path1 = os.path.join(os.path.dirname(video_path), "{}_start{}".format(*basename))
            out_path2 = os.path.join(os.path.dirname(video_path), "{}_end{}".format(*basename))
            self.split_video(video_path, 0, out_path1, max_duration_threshold_secs)
            self.split_video(video_path, int(video_length - max_duration_threshold_secs), out_path2)
            self.concat_videos(out_path1, out_path2, video_path)

    def run_video_synopsis_generation(self, video_paths, max_duration_threshold=10, **kwargs):
        self.run_inference(video_paths, False, 1, max_duration_threshold)
