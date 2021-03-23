#!/usr/bin/env python
# coding: utf8
import cv2
import numpy as np

from airflow.operators.tfserving.base_classes.base_module import BaseModule
from airflow.operators.tfserving.image.pose_detection.models.pose_detection_model.pose_detection_model \
    import PoseDetectionModel
from airflow.operators.tfserving.image.pose_detection.scripts import conf as params
from airflow.operators.tfserving.image.pose_detection.scripts.single_pose_utils import decode_single_pose
from airflow.operators.tfserving.image.pose_detection.scripts.multi_pose_utils import decode_multiple_poses


class PoseDetection(BaseModule):
    """class to return human pose from an image. """

    def __init__(self):
        super().__init__()

    @staticmethod
    def sigmoid(t):
        return 1 / (1 + np.exp(-t))

    def run_inference_per_input(self, frame, pose_detection_model, model_backend=params.default_model_backend,
                                output_stride=params.default_output_stride,
                                max_pose_detections=params.default_max_pose_detections,
                                detection_type=params.default_detection_type):
        img = cv2.cvtColor(cv2.imread(frame), cv2.COLOR_BGR2RGB)
        height, width = img.shape[0], img.shape[1]
        target_height, target_width = (
            ((height // output_stride) * output_stride) + 1,
            ((width // output_stride) * output_stride) + 1
        )
        img = cv2.resize(img, (target_width, target_height))
        output_ratio = (height / target_height, width / target_width)
        if model_backend == 'resnet':
            imagenet_mean = [123.15, 115.90, 103.06]
            img = img - np.array(imagenet_mean)
        elif model_backend == 'mobilenet':
            img = img * (2.0 / 255.0) - 1.0
        else:
            raise ValueError(f"Unknown model backend {model_backend}")
        img = np.expand_dims(img, 0)
        predictions = pose_detection_model.run(img)
        if detection_type == 'single':
            heatmaps, offsets = self.sigmoid(predictions['heatmaps']), predictions['offsets']
            key_point_mapping = decode_single_pose(heatmaps, offsets, output_stride, output_ratio)
        elif detection_type == 'multi':
            heatmaps, offsets, displacement_forward, displacement_backward = (
                self.sigmoid(predictions['heatmaps']),
                predictions['offsets'],
                predictions['displacement_forward'],
                predictions['displacement_backward']
            )

            key_point_mapping = decode_multiple_poses(heatmaps, offsets, displacement_forward, displacement_backward,
                                                      output_stride, max_pose_detections=max_pose_detections,
                                                      output_ratio=output_ratio)
        else:
            raise ValueError(f"Unknown detection type {detection_type}")
        return {frame: key_point_mapping}

    def run_pose_detection(self, frames_list, model_backend=params.default_model_backend,
                           detection_type=params.default_detection_type, output_stride=params.default_output_stride,
                           max_pose_detections=params.default_max_pose_detections, compute_cores=1, synchronous=False,
                           **kwargs):
        """ Performs pose detection on frames.
        :param compute_cores:
        :param frames_list: A list of frames from a video.
        :param model_backend: Name of Model backend to use.
        :param output_stride: Output stride to use for posenet model.
        :param detection_type: Type of detection to use while postprocessing.
        :param max_pose_detections: Maximum number of poses to detect.
        :param synchronous: (Optional) Default True for batching.
        """
        pose_model = PoseDetectionModel(model_backend, output_stride)
        return self.run_inference(frames_list, synchronous, compute_cores, pose_model, model_backend, output_stride,
                                  max_pose_detections, detection_type)
