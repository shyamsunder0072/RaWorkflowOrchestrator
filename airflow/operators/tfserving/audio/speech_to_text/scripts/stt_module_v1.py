import json
import shlex
import subprocess
import sys
import wave

import numpy as np
from airflow.operators.tfserving.audio.speech_to_text.models.version1 import conf as params
from airflow.operators.tfserving.audio.speech_to_text.models.version1 import speech_to_text_model, lm, trie
from deepspeech import Model

try:
    from shhlex import quote
except ImportError:
    from pipes import quote


"""Speech to Text prediction : Returns the top K stt tags prediction using 
segments of audios. 
"""


class STTModule(object):
    """class to return top k stt tags from a audio. """

    @staticmethod
    def convert_samplerate(audio_path, desired_sample_rate):
        sox_cmd = 'sox {} --type raw --bits 16 --channels 1 --rate {} --encoding signed-integer --endian little ' \
                  '--compression 0.0 --no-dither - '.format(quote(audio_path), desired_sample_rate)
        try:
            output = subprocess.check_output(shlex.split(sox_cmd), stderr=subprocess.PIPE)
        except subprocess.CalledProcessError as e:
            raise RuntimeError('SoX returned non-zero status: {}'.format(e.stderr))
        except OSError as e:
            raise OSError(e.errno, 'SoX not found, use {}hz files or install it: {}'.format(
                desired_sample_rate, e.strerror))

        return desired_sample_rate, np.frombuffer(output, np.int16)

    @staticmethod
    def metadata_to_string(metadata):
        return ''.join(item.character for item in metadata.items)

    @staticmethod
    def words_from_metadata(metadata):
        word = ""
        word_list = []
        word_start_time = 0
        # Loop through each character
        for i in range(0, metadata.num_items):
            item = metadata.items[i]
            # Append character to word if it's not a space
            if item.character != " ":
                word = word + item.character
            # Word boundary is either a space or the last character in the array
            if item.character == " " or i == metadata.num_items - 1:
                word_duration = item.start_time - word_start_time

                if word_duration < 0:
                    word_duration = 0

                each_word = dict()
                each_word["word"] = word
                each_word["start_time "] = round(word_start_time, 4)
                each_word["duration"] = round(word_duration, 4)

                word_list.append(each_word)
                # Reset
                word = ""
                word_start_time = 0
            else:
                if len(word) == 1:
                    # Log the start time of the new word
                    word_start_time = item.start_time

        return word_list

    def metadata_json_output(self, metadata):
        json_result = dict()
        json_result["words"] = self.words_from_metadata(metadata)
        json_result["confidence"] = metadata.confidence
        return json.dumps(json_result)
        # run speech to text on a scene audio

    def run_speech_to_text(self, audio_file):
        ds = Model(speech_to_text_model, params.beam_width)
        desired_sample_rate = ds.sampleRate()
        ds.enableDecoderWithLM(lm, trie, params.lm_alpha, params.lm_beta)

        fin = wave.open(audio_file, 'rb')
        fs = fin.getframerate()
        if fs != desired_sample_rate:
            print(
                'Warning: original sample rate ({}) is different than {}hz. Resampling might produce erratic speech '
                'recognition.'.format(
                    fs, desired_sample_rate), file=sys.stderr)
            fs, audio = self.convert_samplerate(audio_file, desired_sample_rate)
        else:
            audio = np.frombuffer(fin.readframes(fin.getnframes()), np.int16)

        fin.close()

        stt_json = self.metadata_json_output(ds.sttWithMetadata(audio))
        stt_string = self.metadata_to_string(ds.sttWithMetadata(audio))
        return stt_string, stt_json
