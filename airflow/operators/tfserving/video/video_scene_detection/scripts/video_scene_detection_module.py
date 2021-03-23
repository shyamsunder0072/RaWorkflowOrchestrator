import os

import pandas as pd
from scenedetect.frame_timecode import FrameTimecode

from airflow.operators.tfserving.base_classes.base_module import BaseModule
from airflow.operators.tfserving.ioutils.directory_io import call_process

""" Returns: video scenes using scenedetect video library
    Detection method : detect-content
    Algorithm used : content detection algorithm
"""


class VideoSceneDetectionModule(BaseModule):
    """class to return visual scenes transition from a video. """

    def __int__(self):
        super().__init__()

    @staticmethod
    def update_scene_number(dataframe):
        for i in range(len(dataframe)):
            dataframe.loc[i, 'Scene Number'] = i + 1
        return dataframe

    @staticmethod
    def split_scene(dataframe, index, maximum_scene_duration):
        """
        Splits large scenes (greater than maximum_scene_duration) into smaller subscenes

        :param dataframe: The original dataframe containing the list of all scenes
        :param index: The index of the scene which must be split
        :param maximum_scene_duration: Maximum length of a subscene (in seconds, float)
        
        :returns: DataFrame containing
        """
        df = dataframe
        i = index
        num_subscenes = int(df['Length (seconds)'][i] / maximum_scene_duration)
        fps = float(df['Length (frames)'][i]) / float(df['Length (seconds)'][i])
        step = FrameTimecode(timecode=float(maximum_scene_duration), fps=fps)
        begin = FrameTimecode(timecode=int(df['Start Frame'][i]), fps=fps)
        end = FrameTimecode(timecode=int(df['End Frame'][i]), fps=fps)
        subscene_list = []

        # Extract subscenes at equal intervals (maximum_scene_duration)
        for s_id in range(num_subscenes):
            start_frame = begin.get_frames()
            start_timecode = begin.get_timecode()
            start_time_seconds = round(begin.get_seconds(), 3)
            end_frame = (begin + step).get_frames()
            end_timecode = (begin + step).get_timecode()
            end_time_seconds = round((begin + step).get_seconds(), 3)
            length_frame = step.get_frames()
            length_timecode = step.get_timecode()
            length_time_seconds = round(step.get_seconds(), 3)
            subscene = {
                'Scene Number': [-1],
                'Start Frame': [start_frame],
                'Start Timecode': [start_timecode],
                'Start Time (seconds)': [start_time_seconds],
                'End Frame': [end_frame],
                'End Timecode': [end_timecode],
                'End Time (seconds)': [end_time_seconds],
                'Length (frames)': [length_frame],
                'Length (timecode)': [length_timecode],
                'Length (seconds)': [length_time_seconds]
            }
            subscene_list.append(pd.DataFrame(subscene))
            begin = begin + step

        # Append the last subscene if it is remaining
        if int(num_subscenes) != float(num_subscenes):
            subscene = {
                'Scene Number': [-1],
                'Start Frame': [begin.get_frames()],
                'Start Timecode': [begin.get_timecode()],
                'Start Time (seconds)': [round(begin.get_seconds(), 3)],
                'End Frame': [end.get_frames()],
                'End Timecode': [end.get_timecode()],
                'End Time (seconds)': [round(end.get_seconds(), 3)],
                'Length (frames)': [(end - begin).get_frames()],
                'Length (timecode)': [(end - begin).get_timecode()],
                'Length (seconds)': [round((end - begin).get_seconds(), 3)]
            }
            subscene_list.append(pd.DataFrame(subscene))
        return pd.concat(subscene_list)

    # get scenes using scene change detection algorithm
    def run_inference_per_input(self, video_file, scene_info_file, minimum_scene_duration="1.0",
                                maximum_scene_duration=-1,
                                algorithm='detect-content', dump_scenes=False, scene_dir=None):
        basename, ext = os.path.splitext(os.path.basename(video_file))
        scene_info_file = scene_info_file.format(basename=basename)
        if not dump_scenes:
            cmd = ["scenedetect",
                   "-i", video_file,
                   "list-scenes",
                   "-f", scene_info_file,
                   algorithm,
                   ]
        else:
            if scene_dir is None:
                scene_dir = os.path.dirname(scene_info_file)
            if not os.path.exists(scene_dir):
                os.makedirs(scene_dir, exist_ok=True)
            cmd = ["scenedetect",
                   "-i", video_file,
                   "-o", scene_dir,
                   "list-scenes",
                   "-f", scene_info_file,
                   algorithm,
                   "split-video"
                   ]
        _ = call_process(cmd)

        if os.path.exists(scene_info_file):
            df = pd.read_csv(scene_info_file, skiprows=[0])

            # Break larger scenes
            if maximum_scene_duration > 0:
                M = float()
                L = []
                for i in range(len(df)):
                    if df['Length (seconds)'][i] <= maximum_scene_duration:
                        df['Length (seconds)'][i] = round(df['Length (seconds)'][i], 3)
                        df['Start Time (seconds)'][i] = round(df['Start Time (seconds)'][i], 3)
                        df['End Time (seconds)'][i] = round(df['End Time (seconds)'][i], 3)
                        L.append(df[df.index == i])
                    else:
                        subscene_list = self.split_scene(dataframe=df, index=i,
                                                         maximum_scene_duration=maximum_scene_duration)
                        L.append(subscene_list)
                df = pd.concat(L)

            # Drop smaller scenes
            df = df[df['Length (seconds)'] >= minimum_scene_duration]
            df.reset_index(drop=True, inplace=True)
            df = self.update_scene_number(df)
            df.to_csv(scene_info_file, index=False)
        else:
            print("[Error] Scene meta data not found for {}, run scene detection module again... ".format(video_file))

    def run_video_scenes_detection(self, video_list, scene_info_file=None, minimum_scene_duration=1.0,
                                   maximum_scene_duration=-1, algorithm='detect-content', dump_scenes=False,
                                   scene_dir=None, **kwargs):
        if scene_dir is None:
            self.run_inference(video_list, False, 1, scene_info_file, minimum_scene_duration, maximum_scene_duration,
                               algorithm, dump_scenes)
        else:
            self.run_inference(video_list, False, 1, scene_info_file, minimum_scene_duration, maximum_scene_duration,
                               algorithm, dump_scenes,
                               scene_dir)
