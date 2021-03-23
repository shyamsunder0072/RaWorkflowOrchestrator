import os

from airflow.operators.tfserving.audio.source_separation.models.version1.source_separation_model import \
    SourceSeparationModel
from airflow.operators.tfserving.audio.source_separation.models.version1 import conf as model_config
from airflow.operators.tfserving.base_classes.base_module import BaseModule


class SourceSeparationModule(BaseModule):

    source_separation_model = SourceSeparationModel()
    codec = model_config.CODEC
    sample_rate = model_config.SAMPLE_RATE
    filename_format = model_config.FILE_FORMAT
    bit_rate = model_config.BIT_RATE

    def __init__(self):
        super().__init__()

    def run_inference_per_input(self, audio, output_path):
        sources = self.source_separation_model.run(audio)
        filename = os.path.splitext(os.path.basename(audio))[0]
        generated = []
        for instrument, data in sources.items():
            sample_output_path = output_path.format(basename=filename, instrument=instrument)
            if sample_output_path == output_path:
                path = os.path.join(output_path, self.filename_format.format(
                    filename=filename,
                    instrument=instrument,
                    codec=self.codec))
            else:
                path = os.path.join(sample_output_path, f"{instrument}.{self.codec}")
            directory = os.path.dirname(path)
            if not os.path.exists(directory):
                os.makedirs(directory)
            if path in generated:
                print(f'Separated source path conflict : {path},'
                      'please check your filename format')
            generated.append(path)
            self.source_separation_model.audio_adapter.save(path, data, self.sample_rate, self.codec, self.bit_rate)

    def run_source_separation(self, audio_files, output_path=None, **kwargs):
        self.run_inference(audio_files, False, 1, output_path)
