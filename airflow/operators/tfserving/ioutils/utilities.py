#!/usr/bin/env python
# coding: utf8

"""
    Utility module for Video Embedding pipeline

    Provides the basic utility functions for few video related tasks.

"""

import ast
import errno
import gc
import multiprocessing
import os
import subprocess
import tempfile
import warnings
from glob import glob
from shutil import rmtree, move
from urllib.parse import urlparse, parse_qs

import pandas as pd

from airflow.operators.tfserving.ioutils import conf as params
from airflow.operators.tfserving.ioutils.directory_io import call_process

warnings.filterwarnings('ignore', category=DeprecationWarning)
warnings.filterwarnings('ignore', category=FutureWarning)
pd.options.mode.chained_assignment = None  # default='warn'


def parse_url(url):
    """get watch id from url"""
    url_data = urlparse(url)
    query = parse_qs(url_data.query)
    file_id = query["v"][0]
    return file_id


def silentremove(filename):
    """remove files and directories as well"""
    try:
        try:
            os.remove(filename)
        except:
            rmtree(filename)
    except OSError as e:  # this would be "except OSError, e:" before Python 2.6
        if e.errno != errno.ENOENT:  # errno.ENOENT = no such file or directory
            raise  # re-raise exception if a different error occurred


def download_video_from_youtube(url, output_file):
    """download youtube video from url and save to disk"""
    cmd = ["youtube-dl",
           "-o", output_file,
           url,
           "--format", "mp4"
           ]

    out = call_process(cmd)
    return out


def convert_video_to_audio(video_file, audio_file, sample_rate=params.audio_sample_rate):
    """convert video to audio
    sample command :  `ffmpeg -i C:/test.mp4 -ab 160k -ac 2 -ar 44100 -vn audio.wav`
    output : 16 bit mono channel and 16khz -> -acodec pcm_s16le -ac 1 -ar 16000
    """
    cmd = ["ffmpeg",
           "-i", video_file,
           "-acodec", "pcm_s16le",
           "-ac", "1",
           "-ar", sample_rate,
           "-vn", audio_file]

    out = call_process(cmd)
    return out


def run_video_to_audio_conversion(argument_list, synchronous=True, compute_cores=params.one_tier_compute_cores):
    if synchronous:
        with multiprocessing.Pool(processes=min(compute_cores, len(argument_list)),
                                  maxtasksperchild=params.tasks_per_child) as task_pool:
            task_pool.starmap(convert_video_to_audio, argument_list)
        gc.collect()
    else:
        for argument in argument_list:
            convert_video_to_audio(*argument)


def convert_audio_rate(input_file, output_file, sample_rate):
    """convert audio sample rate
    sample command :  `ffmpeg -i C:/test.wav -ab 160k -ac 2 -ar 16000  test.wav`
    output : 16 bit mono channel and 16khz -> -acodec pcm_s16le -ac 1 -ar 16000
    """
    basename, ext = os.path.splitext(os.path.basename(output_file))
    temporary_file = tempfile.mktemp(ext)
    temporary_file = os.path.join(os.path.dirname(output_file), os.path.basename(temporary_file))
    cmd = ["ffmpeg",
           "-i", input_file,
           "-acodec", "pcm_s16le",
           "-ac", "1",
           "-ar", sample_rate,
           temporary_file, '-y']

    out = call_process(cmd)
    if os.path.exists(temporary_file):
        move(temporary_file, output_file)
    return out


def get_video_length(filename):
    result = subprocess.run(["ffprobe", "-v", "error", "-show_entries",
                             "format=duration", "-of",
                             "default=noprint_wrappers=1:nokey=1", filename],
                            stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT)
    return float(result.stdout)


def split_video(video_path, start_time, out_path, duration=None):
    if duration:
        cmd = 'ffmpeg -nostats -loglevel error -i "{}" -ss {} -t {} -y "{}"'.format(
            video_path, int(start_time), int(duration), out_path)
    else:
        cmd = 'ffmpeg -nostats -loglevel error -i "{}" -ss {} -y "{}"'.format(
            video_path, int(start_time), out_path)
    os.system(cmd)


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


def get_file_id(input_path):
    file_id = os.path.splitext(os.path.basename(input_path))[0]
    return file_id


def dump_video_ids(output_file, video_ids, category_ids):
    df = pd.DataFrame({params.video_id: video_ids,
                       params.category_id: category_ids})
    df.to_parquet(output_file, index=False)


def dump_video_ids_with_metadata(output_file, video_ids, category_ids, video_titles, video_descriptions):
    df = pd.DataFrame({
        params.video_id: video_ids,
        params.category_id: category_ids,
        params.video_title: video_titles,
        params.video_description: video_descriptions
    })
    df.to_parquet(output_file, index=False)


def dump_batch_info(output_file, video_files):
    if not os.path.exists(output_file):
        df = pd.DataFrame(columns=[params.batch_list, params.time_stamp])
        df.loc[len(df)] = [video_files, pd.to_datetime('now')]
        df.to_parquet(output_file, index=False)
    else:
        df = pd.read_parquet(output_file)
        df.loc[len(df)] = [video_files, pd.to_datetime('now')]
        df.to_parquet(output_file, index=False)


def dump_tags(embeddings_current_run, video_id, k1, v1, k2, v2):
    loaded_tags = pd.read_parquet(embeddings_current_run)
    idx = loaded_tags.index[loaded_tags[params.video_id] == video_id].tolist()[0]
    try:
        loaded_tags[k1][idx] = str(v1)
    except KeyError:
        loaded_tags[k1] = ""
        loaded_tags[k1][idx] = str(v1)
    try:
        loaded_tags[k2][idx] = str(v2)
    except KeyError:
        loaded_tags[k2] = ""
        loaded_tags[k2][idx] = str(v2)

    loaded_tags.to_parquet(embeddings_current_run, index=False)


def get_scene_objects(embeddings_current_run, video_id):
    object_tags = pd.read_parquet(embeddings_current_run)
    object_tags.set_index(params.video_id, inplace=True)
    key_name = params.object_tags_scene_wise
    try:
        scene_objects = ast.literal_eval(object_tags[key_name][video_id])
    except (KeyError, ValueError) as e:
        print('{} : objects not found !'.format(
            video_id
        ))
        scene_objects = ''
    return scene_objects


def get_current_batch(input_file):
    batch_df = pd.read_parquet(input_file)
    return batch_df


def get_directories(full_path, group_by='', filter_dict=None, return_files=False,
                    file_types=None):
    if filter_dict is None:
        filter_dict = {}
    if file_types is None:
        file_types = []
    parts = full_path.split(os.sep)
    glob_path = [os.sep]
    for part in parts:
        if part.startswith('{{') and part.endswith('}}'):
            glob_path.append(part[1:-1])
        elif part.startswith('{') and part.endswith('}'):
            glob_path.append('*')
        else:
            glob_path.append(part)
    glob_path = os.path.join(*glob_path)
    directories = glob(glob_path)
    dynamic_args = []
    for directory in directories:
        arg_dict = {}
        curr_parts = os.path.normpath(directory).split(os.sep)
        for arg, val in zip(parts, curr_parts):
            if arg.startswith('{') and arg.endswith('}') and not arg.startswith('{{') and not arg.endswith('}}'):
                arg_dict[arg[1:-1]] = val
        dynamic_args.append(arg_dict)
    groups = {}
    for directory, args in zip(directories, dynamic_args):
        if return_files == os.path.isdir(directory):
            continue
        _, ext = os.path.splitext(os.path.basename(directory))
        if return_files and ext not in file_types and ext[1:] not in file_types:
            continue
        for k, v in args.items():
            if k in filter_dict.keys() and v not in filter_dict.get(k, []):
                break
        else:
            curr_group = groups.get(args.get(group_by, ''), [])
            curr_group.append((directory, args))
            groups[args.get(group_by, '')] = curr_group
    return list(groups.values())
