import os
import time

import librosa
import numpy as np
from airflow.operators.tfserving.ioutils.directory_io import call_process

""" Returns: audio scenes using librosa audio signal library
    Detection method : onset
    Algorithm used : Segment Agglomerative Clustering
"""


class AudioSceneDetectionModule(object):
    """class to return segments from a audio. """

    @staticmethod
    def run_audio_scene_detection(audio_file, audio_scene_dir, librosa_params):

        samples, sample_rate = librosa.load(audio_file)
        new_sample_rate = librosa_params["new_sample_rate"]
        samples = librosa.resample(samples, sample_rate, new_sample_rate)
        chroma = librosa.feature.chroma_cqt(y=samples, sr=new_sample_rate)
        bounds = librosa.segment.agglomerative(chroma, 20)

        bound_times = np.around(librosa.frames_to_time(bounds, sr=new_sample_rate), decimals=2)
        duration_secs = round(len(samples)/new_sample_rate, 2)

        file_name = audio_file.split('/')[-1].split('.')[0]

        for b in bound_times:
            t1 = b
            if b == max(bound_times):
                t2 = duration_secs
            else:
                t2 = bound_times[np.where(bound_times == b)[0][0]+1]

            start_time = time.strftime('%H:%M:%S', time.gmtime(t1))
            delta_secs = t2-t1
            end_time = time.strftime('%H:%M:%S', time.gmtime(t2))
            segment_name = file_name + "_" + str(start_time) + "_" + str(end_time) + ".wav"
            segment_file = os.path.join(audio_scene_dir, segment_name)

            cmd = ["ffmpeg",
                   "-i", audio_file,
                   "-ss", start_time,
                   "-t", str(delta_secs),
                   "-acodec", "copy",
                   "-y", segment_file]

            _ = call_process(cmd)

        return len(bound_times)
