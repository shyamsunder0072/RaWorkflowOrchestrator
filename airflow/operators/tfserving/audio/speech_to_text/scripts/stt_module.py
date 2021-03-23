"""Speech to Text prediction : Returns the top K stt tags prediction using
segments of audios.
"""

from airflow.operators.tfserving.audio.speech_to_text.models.version2.speech_to_text_model import SpeechToTextModel
from airflow.operators.tfserving.base_classes.base_module import BaseModule


class STTModule(BaseModule):
    """class to return top k stt tags from a audio. """

    speech_to_text_model = SpeechToTextModel()

    def __init__(self):
        super().__init__()

    def run_inference_per_input(self, audio):
        return self.speech_to_text_model.run(audio)

    def run_speech_to_text(self, audio_list, compute_cores=1, synchronous=False, **kwargs):
        """ Performs speech to text on audio segments and returns stt tags.
        :param compute_cores:
        :param audio_list: A list of audio files.
        :param synchronous: (Optional) Default True for batching.
        """
        return self.run_inference(audio_list, synchronous, compute_cores)
