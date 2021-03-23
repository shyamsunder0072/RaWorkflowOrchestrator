#!/usr/bin/env python
# coding: utf8

"""
    Optical Character Recognition.

    Returns the top K ocr tags prediction using extracted frames of videos.

"""

import cv2
import numpy as np

from airflow.operators.tfserving.base_classes.base_module import BaseModule
from airflow.operators.tfserving.image.text_boxes_detection.models import \
    run_text_boxes_detection_model
from airflow.operators.tfserving.image.text_boxes_detection.scripts import conf as config


class TextBoxesDetectionModule(BaseModule):
    """A wrapper class for performing optical character recognition. """

    def __init__(self):
        """ Default constructor."""
        super().__init__()
        return

    @staticmethod
    def read_img(image_path, frame_height=config.frame_height, frame_width=config.frame_width):
        img = cv2.imread(image_path)
        if img.shape[0] != frame_height or img.shape[1] != frame_width:
            img = cv2.resize(img, (frame_width, frame_height))
        return cv2.cvtColor(img, cv2.COLOR_BGR2RGB)

    @staticmethod
    def preprocess_image(image):
        image = image.astype('float32')
        mean = np.array([0.485, 0.456, 0.406])
        variance = np.array([0.229, 0.224, 0.225])

        image -= mean * 255
        image /= variance * 255
        return image

    @staticmethod
    def get_boxes(y_pred, detection_threshold=0.7, text_threshold=0.4, link_threshold=0.4, size_threshold=10):
        box_groups = []
        for y_pred_cur in y_pred:
            textmap = y_pred_cur[..., 0].copy()
            linkmap = y_pred_cur[..., 1].copy()
            img_h, img_w = textmap.shape

            _, text_score = cv2.threshold(textmap, thresh=text_threshold, maxval=1, type=cv2.THRESH_BINARY)
            _, link_score = cv2.threshold(linkmap, thresh=link_threshold, maxval=1, type=cv2.THRESH_BINARY)
            n_components, labels, stats, _ = cv2.connectedComponentsWithStats(
                np.clip(text_score + link_score, 0, 1).astype('uint8'),
                connectivity=4
            )
            boxes = []
            for component_id in range(1, n_components):
                size = stats[component_id, cv2.CC_STAT_AREA]
                if size < size_threshold:
                    continue
                if np.max(textmap[labels == component_id]) < detection_threshold:
                    continue
                #
                segmap = np.zeros_like(textmap)
                segmap[labels == component_id] = 255
                segmap[np.logical_and(link_score, text_score)] = 0
                x, y, w, h = [
                    stats[component_id, key] for key in
                    [cv2.CC_STAT_LEFT, cv2.CC_STAT_TOP, cv2.CC_STAT_WIDTH, cv2.CC_STAT_HEIGHT]
                ]
                niter = int(np.sqrt(size * min(w, h) / (w * h)) * 2)
                sx, sy = max(x - niter, 0), max(y - niter, 0)
                ex, ey = min(x + w + niter + 1, img_w), min(y + h + niter + 1, img_h)
                segmap[sy:ey, sx:ex] = cv2.dilate(
                    segmap[sy:ey, sx:ex],
                    cv2.getStructuringElement(cv2.MORPH_RECT, (1 + niter, 1 + niter)))
                contours = cv2.findContours(segmap.astype('uint8'),
                                            mode=cv2.RETR_TREE,
                                            method=cv2.CHAIN_APPROX_SIMPLE)[-2]
                contour = contours[0]
                box = cv2.boxPoints(cv2.minAreaRect(contour))
                w, h = np.linalg.norm(box[0] - box[1]), np.linalg.norm(box[1] - box[2])
                box_ratio = max(w, h) / (min(w, h) + 1e-5)
                if abs(1 - box_ratio) <= 0.1:
                    l, r = contour[:, 0, 0].min(), contour[:, 0, 0].max()
                    t, b = contour[:, 0, 1].min(), contour[:, 0, 1].max()
                    box = np.array([[l, t], [r, t], [r, b], [l, b]], dtype=np.float32)
                else:
                    box = np.array(np.roll(box, 4 - box.sum(axis=1).argmin(), 0))
                boxes.append(2 * box)
            box_groups.append(np.array(boxes))
        return box_groups

    def run_inference_per_input(self, batch_frames, frame_height=config.frame_height, frame_width=config.frame_width):
        """ Performs text recognition on video frame.

        :param batch_frames:  Path to batch of raw video frames.
        :param frame_height: Target height of each frame.
        :param frame_width: Target width of each frame.
        :return         Target text as a result.
        """
        images = np.array([self.preprocess_image(self.read_img(i, frame_height=frame_height, frame_width=frame_width))
                           for i in batch_frames], dtype=np.float32)
        box_groups = self.get_boxes(run_text_boxes_detection_model(images))
        return [{img: boxes} for img, boxes in zip(batch_frames, box_groups)]

    def run_text_boxes_detection(self, frames_list, compute_cores=1, synchronous=True, frame_height=config.frame_height,
                                 frame_width=config.frame_width, batch_size=config.batch_size, **kwargs):
        """ Performs optical character detection on frames and dumps bound boxes.

        :param compute_cores:
        :param frames_list: A list of frames from a video.
        :param synchronous: (Optional) Default True for batching.
        :param frame_height:
        :param frame_width:
        :param batch_size
        """
        batch_argument_list = [frames_list[i:(i + batch_size)]
                               for i in range(0, len(frames_list), batch_size)]
        result = self.run_inference(batch_argument_list, synchronous, compute_cores, frame_height, frame_width)
        result_list = [frame_output for batch_output in result for frame_output in batch_output]
        return result_list
