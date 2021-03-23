import io
import os

import cv2
import numpy as np
import pandas as pd

from airflow.operators.tfserving.base_classes.base_module import BaseModule
from airflow.operators.tfserving.ioutils.opencv_custom_module import stderr_redirector
from airflow.operators.tfserving.video.rgb_streams_computation.scripts import conf as params


class RgbStreamComputation(BaseModule):
    def __init__(self):
        super().__init__()

    @staticmethod
    def run_inference_per_input(video_path, scene_info_file, stream_dir, scene_level_objects):
        """Compute RGB from for all scenes of a full video """
        video_id = os.path.splitext(os.path.basename(video_path))[0]
        scene_info_file = scene_info_file.format(basename=video_id)
        stream_dir = stream_dir.format(basename=video_id)
        if not scene_level_objects or type(scene_level_objects) is not dict:
            scene_level_objects = None
        df = pd.read_csv(scene_info_file)
        if len(df) == 0:
            print("[Info] No scenes to process : {}".format(video_id))
        else:
            print("[Info] Processing {}".format(video_id))
            start_frames, end_frames = df['Start Frame'], df['End Frame']
            start_times, end_times = df['Start Timecode'], df['End Timecode']
            rgb = []
            frame_ix, scene_ix = 0, 0
            s_frame, e_frame = start_frames[scene_ix], end_frames[scene_ix]
            f = io.StringIO()
            if not os.path.exists(stream_dir):
                os.makedirs(stream_dir, exist_ok=True)
            with stderr_redirector(f):
                vidcap = cv2.VideoCapture(video_path)
                success, frame = vidcap.read()
                while success or scene_ix <= len(df) - 1:
                    try:
                        scene_name = start_times[scene_ix] + "_" + end_times[scene_ix]
                        out_file = os.path.join(stream_dir, scene_name + params.stream_ext)
                        if not os.path.exists(out_file):
                            if scene_level_objects is not None:
                                if scene_name in scene_level_objects.keys():
                                    scene_ix_object = scene_level_objects[scene_name]
                                    to_check_person = [x[0] for x in scene_ix_object]
                                    if "person" in to_check_person:
                                        if s_frame <= frame_ix <= e_frame:
                                            if frame is not None:
                                                frame = cv2.resize(frame, (342, 256))
                                                # frame = (frame / 255.) * 2 - 1
                                                frame = frame[16:240, 59:283]
                                                if len(rgb) < params.rgb_stream_shape:
                                                    rgb.append(frame)
                                            if frame_ix == e_frame:
                                                while len(rgb) < params.rgb_stream_shape:
                                                    len_to_add = params.rgb_stream_shape - len(rgb)
                                                    rgb.extend(rgb[:len_to_add])
                                                rgb = rgb[:params.rgb_stream_shape]
                                                rgb = np.asarray([np.array(rgb)])
                                                out_file = os.path.join(stream_dir,
                                                                        start_times[scene_ix] + "_" + end_times[
                                                                            scene_ix] + params.stream_ext)
                                                np.save(out_file, rgb)
                                                rgb = []
                                                scene_ix += 1
                                                if scene_ix <= len(df) - 1:
                                                    s_frame, e_frame = start_frames[scene_ix], end_frames[scene_ix]
                                    else:
                                        scene_ix += 1
                                        if scene_ix <= len(df) - 1:
                                            s_frame, e_frame = start_frames[scene_ix], end_frames[scene_ix]
                                else:
                                    scene_ix += 1
                                    if scene_ix <= len(df) - 1:
                                        s_frame, e_frame = start_frames[scene_ix], end_frames[scene_ix]
                            else:
                                if s_frame <= frame_ix <= e_frame:
                                    if frame is not None:
                                        frame = cv2.resize(frame, (342, 256))
                                        # frame = (frame / 255.) * 2 - 1
                                        frame = frame[16:240, 59:283]
                                        if len(rgb) < params.rgb_stream_shape:
                                            rgb.append(frame)
                                    if frame_ix == e_frame:
                                        while len(rgb) < params.rgb_stream_shape:
                                            len_to_add = params.rgb_stream_shape - len(rgb)
                                            rgb.extend(rgb[:len_to_add])
                                        rgb = rgb[:params.rgb_stream_shape]
                                        rgb = np.asarray([np.array(rgb)])
                                        out_file = os.path.join(stream_dir,
                                                                start_times[scene_ix] + "_" + end_times[
                                                                    scene_ix] + params.stream_ext)
                                        np.save(out_file, rgb)
                                        rgb = []
                                        scene_ix += 1
                                        if scene_ix <= len(df) - 1:
                                            s_frame, e_frame = start_frames[scene_ix], end_frames[scene_ix]
                                # else:
                                #     scene_ix += 1
                                #     if scene_ix <= len(df) - 1:
                                #         s_frame, e_frame = start_frames[scene_ix], end_frames[scene_ix]
                        else:
                            scene_ix += 1
                            if scene_ix <= len(df) - 1:
                                s_frame, e_frame = start_frames[scene_ix], end_frames[scene_ix]
                    except KeyError:
                        pass
                    frame_ix += 1
                    success, frame = vidcap.read()
                vidcap.release()
            f.close()

    def run_rgb_streams_computation(self, video_list, scene_info_file=None, stream_dir=None, scene_level_objects=None,
                                    **kwargs):
        self.run_inference(video_list, False, 1,  scene_info_file, stream_dir, scene_level_objects)
