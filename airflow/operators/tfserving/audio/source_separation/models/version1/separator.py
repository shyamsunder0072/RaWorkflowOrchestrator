#!/usr/bin/env python
# coding: utf8

"""
    Module that provides a class wrapper for source separation.

    :Example:

     from airflow.operators.tfserving.audio.source_separation.separator import Separator
     separator = Separator()
     separator.separate(waveform, lambda instrument, data: ...)
     separator.separate_to_file(...)

"""

import os
from multiprocessing import Pool
from os.path import basename, join, splitext

import tensorflow as tf
from airflow.operators.tfserving.audio.source_separation.models.version1 import conf as model_config
from airflow.operators.tfserving.grpc_client_utils import ProdClient
from airflow.operators.tfserving.grpc_client_utils import conf as grpc_config

from airflow.operators.tfserving.audio.source_separation.models.version1.utilities import get_default_audio_adapter, \
    to_stereo


class Separator(object):
    """ A wrapper class for performing separation. """

    _HOST = grpc_config.TF_SERVING_HOST
    _MODEL = model_config.MODEL_NAME
    _VERSION = model_config.MODEL_VERSION

    _TENSOR_NAME = grpc_config.INPUT_TENSOR_NAME
    _TENSOR_DTYPE = grpc_config.INPUT_TENSOR_DTYPE
    _INPUT_DATA = grpc_config.INPUT_DATA

    def __init__(self, multiprocess=False):
        """ Default constructor."""

        self._sample_rate = model_config.SAMPLE_RATE
        self._pool = Pool(model_config.cores_per_task) if multiprocess else None
        self._tasks = []

        self.client = ProdClient(self._HOST, self._MODEL, self._VERSION)

    def join(self, timeout=200):
        """ Wait for all pending tasks to be finished.

        :param timeout: (Optional) task waiting timeout.
        """
        while len(self._tasks) > 0:
            task = self._tasks.pop()
            task.get()
            task.wait(timeout=timeout)

    def separate(self, waveform):
        """ Performs source separation over the given waveform.

        The separation is performed synchronously but the result
        processing is done asynchronously, allowing for instance
        to export audio in parallel (through multiprocessing).

        Given result is passed by to the given consumer, which will
        be waited for task finishing if synchronous flag is True.

        :param waveform: Waveform to apply separation on.
        :returns: Separated waveforms.
        """
        if not waveform.shape[-1] == 2:
            waveform = to_stereo(waveform)
        req_data = {self._TENSOR_NAME: 'waveform', self._TENSOR_DTYPE: tf.float32, self._INPUT_DATA: waveform}
        prediction = self.client.predict(req_data)
        return prediction

    def separate_to_file(
            self, audio_descriptor, destination,
            audio_adapter=get_default_audio_adapter,
            offset=0, duration=600., codec='wav', bitrate='128k',
            filename_format='{filename}/{instrument}.{codec}',
            synchronous=True):
        """ Performs source separation and export result to file using
        given audio adapter.

        Filename format should be a Python formattable string that could use
        following parameters : {instrument}, {filename} and {codec}.

        :param audio_descriptor:    Describe song to separate, used by audio
                                    adapter to retrieve and load audio data,
                                    in case of file based audio adapter, such
                                    descriptor would be a file path.
        :param destination:         Target directory to write output to.
        :param audio_adapter:       (Optional) Audio adapter to use for I/O.
        :param offset:              (Optional) Offset of loaded song.
        :param duration:            (Optional) Duration of loaded song.
        :param codec:               (Optional) Export codec.
        :param bitrate:             (Optional) Export bitrate.
        :param filename_format:     (Optional) Filename format.
        :param synchronous:         (Optional) True is should by synchronous.
        """
        waveform, _ = audio_adapter.load(
            audio_descriptor,
            offset=offset,
            duration=duration,
            sample_rate=self._sample_rate)
        sources = self.separate(waveform)
        filename = splitext(basename(audio_descriptor))[0]
        generated = []
        for instrument, data in sources.items():
            path = join(destination, filename_format.format(
                filename=filename,
                instrument=instrument,
                codec=codec))
            directory = os.path.dirname(path)
            if not os.path.exists(directory):
                os.makedirs(directory)
            if path in generated:
                print(f'Separated source path conflict : {path},'
                      'please check your filename format')
            generated.append(path)
            if self._pool:
                task = self._pool.apply_async(audio_adapter.save, (
                    path,
                    data,
                    self._sample_rate,
                    codec,
                    bitrate))
                self._tasks.append(task)
            else:
                audio_adapter.save(path, data, self._sample_rate, codec, bitrate)
        if synchronous and self._pool:
            self.join()
