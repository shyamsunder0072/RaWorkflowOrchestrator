#!/usr/bin/env python
# coding: utf8

""" Semantic Segmentation - Video Scene Parsing.

    Returns the top K object tags prediction using
    extracted frames of videos. It also makes improvements by post processing
    on model predictions.

"""

from airflow.operators.tfserving.base_classes.base_module import BaseModule
from airflow.operators.tfserving.image.semantic_segmentation.models.version1.semantic_segmentation_model import \
    SemanticSegmentationModel


class ObjectModule(BaseModule):
    """class to return object predictions from a video frame. """

    semantic_segmentation_model = SemanticSegmentationModel()

    def __init__(self):
        super().__init__()

    def run_inference_per_input(self, frame, return_seg_map=False):
        prediction_dict, segmentation_map_dict = self.semantic_segmentation_model.run(frame)
        if return_seg_map:
            output_dict = {}
            for k in prediction_dict.keys():
                prediction = prediction_dict.get(k, dict())
                seg_map = segmentation_map_dict.get(k, [[]])
                output_dict[k] = (prediction, seg_map)
            return output_dict
        else:
            return prediction_dict

    def run_semantic_segmentation(self, frames_list, return_seg_map=False, compute_cores=1, synchronous=False, **kwargs):
        """ Performs semantic segmentation on frames and returns
        a list of parsed objects with a confidence value.
        :param compute_cores:
        :param frames_list: A list of frames from a video.
        :param return_seg_map: Boolean to return the segmentation map
        :param synchronous: (Optional) Default True for batching.
        """
        return self.run_inference(frames_list, synchronous, compute_cores, return_seg_map)
