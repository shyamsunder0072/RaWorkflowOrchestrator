#!/usr/bin/env python
# coding: utf8

from airflow.operators.tfserving.base_classes.base_module import BaseModule
from airflow.operators.tfserving.image.object_detection.models.object_detection_model.object_detection_model import \
    ObjectDetectionModel
from airflow.operators.tfserving.image.object_detection.scripts import conf as params


class ObjectDetection(BaseModule):
    """class to return object predictions from an image. """

    model = ObjectDetectionModel

    def __init__(self):
        super().__init__()

    def run_inference_per_input(self, frame, object_detection_model, confidence_threshold, dump_bounding_boxes=False):
        detection_classes, detection_scores, detection_boxes = object_detection_model.run(frame)
        output = {}
        for label, s, box in zip(detection_classes, detection_scores, detection_boxes):
            if round(s, 2) >= confidence_threshold:
                if dump_bounding_boxes:
                    output[label] = output.get(label, []) + [(round(s, 2), box.tolist())]
                else:
                    output[label] = output.get(label, []) + [round(s, 2)]
        return {frame: output}

    def run_object_detection(self, frames_list, model_name=params.default_model,
                             confidence_threshold=params.confidence_threshold, dump_bounding_boxes=False,
                             compute_cores=1, synchronous=False,
                             **kwargs):
        """ Performs object detection on frames and returns
        a list of parsed objects with a confidence value.
        :param compute_cores:
        :param frames_list: A list of frames from a video.
        :param model_name: Name of the object detection model to use.
        :param confidence_threshold:
        :param dump_bounding_boxes:
        :param synchronous: (Optional) Default True for batching.
        """
        object_detection_model = ObjectDetectionModel(model_name)
        return self.run_inference(frames_list, synchronous, compute_cores, object_detection_model, confidence_threshold,
                                  dump_bounding_boxes)
