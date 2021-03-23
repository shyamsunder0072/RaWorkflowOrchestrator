import gc
import glob
import multiprocessing
import os
import subprocess

import cv2
import numpy as np
import pandas as pd
from scipy.signal import argrelextrema
from skimage.metrics import structural_similarity

from airflow.operators.tfserving.base_classes.base_module import BaseModule
from airflow.operators.tfserving.ioutils.directory_io import call_process, create_directory


class Frame:
    def __init__(self, id_, diff):
        self.id = id_
        self.diff = diff

    def __lt__(self, other):
        if self.id == other.id:
            return self.id < other.id
        return self.id < other.id

    def __gt__(self, other):
        return other.__lt__(self)

    def __eq__(self, other):
        return self.id == other.id and self.id == other.id

    def __ne__(self, other):
        return not self.__eq__(other)


class KeyFramesExtraction(BaseModule):
    def __init__(self):
        super().__init__()

    @staticmethod
    def run_frames_extraction_using_fps_per_video(video_path, output_dir, fps, scene_file=None):
        """extract frames from each scene at certain fps
        and save them to disk
        sample command : `ffmpeg -i test-multi-1.mp4 -ss 00:00:12 -t 1  -vf fps=2 frames%d.png`
        output : dictionary containing scene wise number of frames generated
        """
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        video_id = os.path.splitext(os.path.basename(video_path))[0]
        if scene_file:
            scene_df = pd.read_csv(scene_file)
            if len(scene_df) == 0:
                print("[fps-method] No scenes to process : {}".format(video_id))
            else:
                print("[fps-method] Processing {}".format(video_id))
                start_times, durations = scene_df['Start Timecode'], scene_df['Length (timecode)']
                end_times = scene_df['End Timecode']

                for i in range(len(scene_df)):
                    scene_name = start_times[i] + "_" + end_times[i]
                    sub_frames_dir = os.path.join(output_dir, scene_name)
                    create_directory(sub_frames_dir)
                    output_path = sub_frames_dir + '/frames%03d.jpg'
                    cmd = ["ffmpeg",
                           "-ss", start_times[i],
                           "-i", video_path,
                           "-crf", "20",
                           "-t", durations[i],
                           "-vf",
                           "fps=" + str(fps),
                           output_path,
                           "-hide_banner"
                           ]
                    _ = call_process(cmd)
        else:
            # sub_frames_dir = os.path.join(output_dir, 'full_video')
            # create_directory(sub_frames_dir)
            # output_path = sub_frames_dir + '/frames%03d.jpg'
            output_path = output_dir + '/frames%03d.jpg'
            cmd = ["ffmpeg",
                   "-i", video_path,
                   "-crf", "20",
                   "-vf",
                   "fps=" + str(fps),
                   output_path,
                   "-hide_banner"
                   ]
            _ = call_process(cmd)
            num_frames = len(glob.glob(output_dir + '/*.jpg'))
            print('\nFrames -> {} : {}'.format(
                output_dir, num_frames))

    @staticmethod
    def smooth(x, window_len=13, window='hanning'):
        s = np.r_[2 * x[0] - x[window_len:1:-1],
                  x, 2 * x[-1] - x[-1:-window_len:-1]]
        if window == 'flat':
            w = np.ones(window_len, 'd')
        else:
            w = getattr(np, window)(window_len)
        y = np.convolve(w / w.sum(), s, mode='same')
        return y[window_len - 1:-window_len + 1]

    @staticmethod
    def get_intra_coded_frames(scene):
        command = 'ffprobe -v error -show_entries frame=pict_type -of default=noprint_wrappers=1'.split()
        out = subprocess.check_output(command + [scene]).decode()
        frame_types = out.replace('pict_type=', '').split()
        frame_types = zip(range(len(frame_types)), frame_types)
        i_frames = [x[0] for x in frame_types if x[1] == 'I']
        return i_frames

    def run_frames_extraction_using_ffprobe(self, scene, sub_frames_dir):
        i_frames = self.get_intra_coded_frames(scene)

        if i_frames:
            cap = cv2.VideoCapture(scene)
            for frame_no in i_frames:
                cap.set(cv2.CAP_PROP_POS_FRAMES, frame_no)
                ret, frame = cap.read()
                frame_filename = 'keyframe_' + str(frame_no) + '.jpg'
                output_path = os.path.join(sub_frames_dir, frame_filename)
                cv2.imwrite(output_path, frame)
                print('ffprobe dumping : ' + output_path)
            cap.release()

        else:
            print('ffprobe dumping : No key frames detected in ' + scene)

    def run_key_frame_extraction_scene(self, video_path, s_frame, e_frame, sub_frames_dir, len_window=50,
                                       variance_threshold=10, ssi_threshold=0.88, transition_threshold=7):
        # print("Processing Scene", s_frame, e_frame)
        curr_frame = None
        prev_frame = None
        frame_diffs = []
        frames = []
        i = 0
        start, end = s_frame, e_frame
        vidcap = cv2.VideoCapture(video_path)
        vidcap.set(cv2.CAP_PROP_POS_FRAMES, s_frame)
        while start < end:
            success, frame = vidcap.read()
            if not success:
                break
            luv = cv2.cvtColor(frame, cv2.COLOR_BGR2LUV)
            curr_frame = luv
            if curr_frame is not None and prev_frame is not None:
                diff = cv2.absdiff(curr_frame, prev_frame)
                diff_sum = np.sum(diff)
                diff_sum_mean = diff_sum / (diff.shape[0] * diff.shape[1])
                frame_diffs.append(diff_sum_mean)
                frame_diff = Frame(i, diff_sum_mean)
                frames.append(frame_diff)
            prev_frame = curr_frame
            i = i + 1
            start += 1
        # compute keyframe
        keyframe_id_set = set()
        diff_array = np.array(frame_diffs)
        if len(diff_array) == 0:
            return
        sm_diff_array = self.smooth(diff_array, len_window)
        frame_indexes = np.asarray(argrelextrema(sm_diff_array, np.greater))[0]
        for i in frame_indexes:
            keyframe_id_set.add(frames[i - 1].id)
        if not os.path.exists(sub_frames_dir):
            os.mkdir(sub_frames_dir)
        idx = 0
        prev_frame, prev_name = None, None
        vidcap.release()
        vidcap2 = cv2.VideoCapture(video_path)
        vidcap2.set(cv2.CAP_PROP_POS_FRAMES, s_frame)
        while s_frame < e_frame:
            success, frame = vidcap2.read()
            if not success:
                break
            if idx in keyframe_id_set:
                name = "keyframe_" + str(idx) + ".jpg"
                if np.std(frame) > variance_threshold:
                    if prev_frame is not None:
                        ssim = structural_similarity(prev_frame, frame, multichannel=True)
                        if ssim < ssi_threshold:
                            cv2.imwrite(prev_name, prev_frame)
                            prev_frame, prev_name = frame, os.path.join(sub_frames_dir, name)
                        elif np.mean(cv2.subtract(prev_frame, frame)) < transition_threshold:
                            prev_frame, prev_name = frame, os.path.join(sub_frames_dir, name)
                    else:
                        prev_frame, prev_name = frame, os.path.join(sub_frames_dir, name)
                keyframe_id_set.remove(idx)
            s_frame += 1
            idx += 1
        if prev_frame is not None:
            cv2.imwrite(prev_name, prev_frame)

    def run_frames_extraction_using_hist_per_video(self, video_path, scene_info_file, key_frames_dir, window_len,
                                                   synchronous, compute_cores):
        if not os.path.exists(key_frames_dir):
            os.makedirs(key_frames_dir, exist_ok=True)
        video_id = os.path.splitext(os.path.basename(video_path))[0]
        df = pd.read_csv(scene_info_file)
        num_scenes = len(os.listdir(key_frames_dir))
        if len(df) == 0:
            print("[hist-method] No scenes to process : {}".format(video_id))
        elif num_scenes == len(df):
            print("[hist-method] All scenes already processed : {}".format(video_id))
        else:
            print("[hist-method] Processing {}".format(video_id))
            start_frames, end_frames = df['Start Frame'].values, df['End Frame'].values
            start_times, end_times = df['Start Timecode'].values, df['End Timecode'].values
            argument_list = []
            for s_frame, e_frame, s_time, e_time in zip(start_frames, end_frames, start_times, end_times):
                scene_name = s_time + "_" + e_time
                argument_list.append((video_path, s_frame, e_frame, os.path.join(key_frames_dir, scene_name),
                                      window_len))
            if synchronous:
                with multiprocessing.Pool(processes=min(len(argument_list), compute_cores), maxtasksperchild=1000) as pool:
                    pool.starmap(self.run_key_frame_extraction_scene, argument_list)
                gc.collect()
            else:
                for argument in argument_list:
                    self.run_key_frame_extraction_scene(*argument)

    def run_inference_per_input(self, video, algorithm, scene_info_file, output_path, fps=1, window_len=50,
                                synchronous=False, compute_cores=1):
        basename, ext = os.path.splitext(os.path.basename(video))
        scene_info_file = scene_info_file.format(basename=basename)
        output_path = output_path.format(basename=basename)
        if algorithm == 'hist':
            self.run_frames_extraction_using_hist_per_video(video, scene_info_file, output_path, window_len,
                                                            synchronous, compute_cores)
        elif algorithm == 'fps':
            self.run_frames_extraction_using_fps_per_video(video, output_path, fps, scene_info_file)

    def run_frames_extraction_using_hist(self, input_list, scene_info_file=None, output_path=None, compute_cores=1,
                                         synchronous=False, fps=1, window_len=50, **kwargs):
        self.run_inference(input_list, False, 1, 'hist', scene_info_file, output_path, fps, window_len, synchronous,
                           compute_cores)

    def run_frames_extraction_using_fps(self, input_list, scene_info_file=None, output_path=None, fps=1, **kwargs):
        self.run_inference(input_list, False, 1, 'fps', scene_info_file, output_path, fps)
