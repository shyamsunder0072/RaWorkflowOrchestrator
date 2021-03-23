import os
from auditok import AudioRegion

from airflow.operators.tfserving.base_classes.base_module import BaseModule


class AudioSceneDetectionModule(BaseModule):
    def __init__(self):
        super().__init__()
        return

    @staticmethod
    def run_inference_per_input(input_file, output_path):
        region_full = AudioRegion.load(input_file)
        regions = region_full.split(min_dur=1.0, max_dur=10.0, max_silence=0.5)
        print("[Info] Dumping aural scenes for {}".format(input_file))
        basename, ext = os.path.splitext(os.path.basename(input_file))
        output_path = output_path.format(basename=basename)
        if not os.path.exists(output_path):
            os.makedirs(output_path, exist_ok=True)
        for region in regions:
            region.save(os.path.join(output_path, "{meta.start:.3f}_{meta.end:.3f}.wav"))

    def run_audio_scene_detection(self, audio_list, output_path=None, **kwargs):
        self.run_inference(audio_list, False, 1, output_path)
