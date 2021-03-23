import os
import ffmpeg
import numpy as np


def _to_ffmpeg_time(n):
    """ Format number of seconds to time expected by FFMPEG.
    :param n: Time in seconds to format.
    :returns: Formatted time in FFMPEG format.
    """
    m, s = divmod(n, 60)
    h, m = divmod(m, 60)
    return '%d:%02d:%09.6f' % (h, m, s)


def _to_ffmpeg_codec(codec):
    ffmpeg_codecs = {
        'm4a': 'aac',
        'ogg': 'libvorbis',
        'wma': 'wmav2',
    }
    return ffmpeg_codecs.get(codec) or codec


def to_stereo(waveform):
    """ Convert a waveform to stereo by duplicating if mono,
    or truncating if too many channels.

    :param waveform: a (N, d) numpy array.
    :returns: A stereo waveform as a (N, 1) numpy array.
    """
    if waveform.shape[1] == 1:
        return np.repeat(waveform, 2, axis=-1)
    if waveform.shape[1] > 2:
        return waveform[:, :2]
    return waveform


class FFMPEGProcessAudioAdapter:
    """ An AudioAdapter implementation that use FFMPEG binary through
    subprocess in order to perform I/O operation for audio processing.

    When created, FFMPEG binary path will be checked and expended,
    raising exception if not found. Such path could be infered using
    FFMPEG_PATH environment variable.
    """
    @staticmethod
    def load(path, offset=None, duration=None, sample_rate=None, dtype=np.float32):
        """ Loads the audio file denoted by the given path
        and returns it data as a waveform.

        :param path: Path of the audio file to load data from.
        :param offset: (Optional) Start offset to load from in seconds.
        :param duration: (Optional) Duration to load in seconds.
        :param sample_rate: (Optional) Sample rate to load audio with.
        :param dtype: (Optional) Numpy data type to use, default to float32.
        :returns: Loaded data a (waveform, sample_rate) tuple.
        """
        if not isinstance(path, str):
            path = path.decode()
        probe = None
        try:
            probe = ffmpeg.probe(path)
        except ffmpeg._run.Error as e:
            print(
                'An error occurs with ffprobe (see ffprobe output below)\n\n{}'
                .format(e.stderr.decode()))
        if 'streams' not in probe or len(probe['streams']) == 0:
            print('No stream was found with ffprobe')
        metadata = next(
            stream
            for stream in probe['streams']
            if stream['codec_type'] == 'audio')
        n_channels = metadata['channels']
        if sample_rate is None:
            sample_rate = metadata['sample_rate']
        output_kwargs = {'format': 'f32le', 'ar': sample_rate}
        if duration is not None:
            output_kwargs['t'] = _to_ffmpeg_time(duration)
        if offset is not None:
            output_kwargs['ss'] = _to_ffmpeg_time(offset)
        process = (
            ffmpeg
            .input(path)
            .output('pipe:', **output_kwargs)
            .run_async(pipe_stdout=True, pipe_stderr=True))
        buffer, _ = process.communicate()
        waveform = np.frombuffer(buffer, dtype='<f4').reshape(-1, n_channels)
        if not waveform.dtype == np.dtype(dtype):
            waveform = waveform.astype(dtype)
        return waveform, sample_rate

    @staticmethod
    def save(path, data, sample_rate, codec=None, bit_rate=None):
        """ Write waveform data to the file denoted by the given path
        using FFMPEG process.

        :param path: Path of the audio file to save data in.
        :param data: Waveform data to write.
        :param sample_rate: Sample rate to write file in.
        :param codec: (Optional) Writing codec to use.
        :param bit_rate: (Optional) Bit rate of the written audio file.
        :raise IOError: If any error occurs while using FFMPEG to write data.
        """
        directory = os.path.dirname(path)
        if not os.path.exists(directory):
            print(f'output directory does not exists: {directory}')
        input_kwargs = {'ar': sample_rate, 'ac': data.shape[1]}
        output_kwargs = {'ar': sample_rate, 'strict': '-2'}
        if bit_rate:
            output_kwargs['audio_bitrate'] = bit_rate
        if codec is not None and codec != 'wav':
            output_kwargs['codec'] = _to_ffmpeg_codec(codec)
        process = (
            ffmpeg
            .input('pipe:', format='f32le', **input_kwargs)
            .output(path, **output_kwargs)
            .overwrite_output()
            .run_async(pipe_stdin=True, pipe_stderr=True, quiet=True))
        try:
            process.stdin.write(data.astype('<f4').tobytes())
            process.stdin.close()
            process.wait()
        except IOError:
            print(f'FFMPEG error: {process.stderr.read()}')


get_default_audio_adapter = FFMPEGProcessAudioAdapter()
