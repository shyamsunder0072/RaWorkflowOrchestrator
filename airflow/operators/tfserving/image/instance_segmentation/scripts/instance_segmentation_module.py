import numpy as np
import os

from airflow.operators.tfserving.base_classes.base_module import BaseModule
from airflow.operators.tfserving.image.instance_segmentation.models.yolact.yolact_model import YolactNetModel


class InstanceSegmentationModule(BaseModule):
    yolact_network = YolactNetModel()

    def __init__(self):
        super().__init__()

    def run_inference_per_input(self, image_path, output_dir):

        mask = self.yolact_network.run(image_path)
        filename = os.path.splitext(os.path.basename(image_path))[0]
        output_dir = output_dir.format(basename=filename)

        if not os.path.exists(output_dir):
            os.makedirs(output_dir, exist_ok=True)

        mask_name = filename + '.npy'
        np.save(os.path.join(output_dir, mask_name), mask)

    def run_masking(self, image_path_list, compute_cores=1, synchronous=False, output_dir=None, **kwargs):
        self.run_inference(image_path_list, synchronous, compute_cores, output_dir)
