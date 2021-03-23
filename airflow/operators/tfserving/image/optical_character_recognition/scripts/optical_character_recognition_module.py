#!/usr/bin/env python
# coding: utf8

"""
    Optical Character Recognition.

    Returns the top K ocr tags prediction using extracted frames of videos.

"""

import json
import os
import re
import string

import cv2
import nltk
import numpy as np

from airflow.operators.tfserving.base_classes.base_module import BaseModule
from airflow.operators.tfserving.image.optical_character_recognition.models import \
    run_optical_character_recognition_model
from airflow.operators.tfserving.image.optical_character_recognition.scripts import conf as config
from airflow.operators.tfserving.image.optical_character_recognition.scripts import tools


class OpticalCharacterRecognitionModule(BaseModule):
    """A wrapper class for performing optical character recognition. """

    _STOPWORDS = nltk.corpus.stopwords.words('english')
    _LEMMATIZER = nltk.stem.wordnet.WordNetLemmatizer()

    def __init__(self):
        """ Default constructor."""
        self._alphabet = string.digits + string.ascii_lowercase
        self._blank_label_idx = len(self._alphabet)
        super().__init__()

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

    def apply_text_cleaning(self, video_text):
        """ Performs post processing on video texts.

        :param video_text:  List of raw video level texts.
        :return:            List of processed video level texts.
        """
        v = video_text[:]
        v = [re.sub("[^a-zA-Z]", '', text) for text in v]
        v = [el for el in v if len(el) > 2]
        v = [el.lower() for el in v]
        v = [el for el in v if el not in self._STOPWORDS]
        v = [self._LEMMATIZER.lemmatize(el) for el in v]
        return v

    @staticmethod
    def sort_tuple(tup):
        tup.sort(key=lambda x: x[1])
        return tup

    def align_text(self, raw_box_groups, prediction_text):
        if len(prediction_text) == 0:
            return []
        sortval = raw_box_groups[:, 3, 1]
        sorted_idx = np.argsort(sortval)
        box_groups = raw_box_groups[sorted_idx]
        align_text = ''
        line = []
        coordinate_threshold = 5
        for index in range(len(box_groups)):
            if np.all(box_groups[index] > 0):
                if len(line) > 0:
                    if box_groups[index, 3, 1] <= (box_groups[index - len(line), 3, 1] + coordinate_threshold) or (
                            box_groups[index, 3, 1] + coordinate_threshold) <= box_groups[index - len(line), 3, 1]:
                        line.append((prediction_text[sorted_idx[index]], box_groups[index, 3, 0]))

                    else:
                        sort_line = self.sort_tuple(line)
                        for word_num in range(len(sort_line)):
                            align_text = align_text + ' ' + sort_line[word_num][0]
                        line = []
                        line.append((prediction_text[sorted_idx[index]], box_groups[index, 3, 0]))

                else:
                    line.append((prediction_text[sorted_idx[index]], box_groups[index, 3, 0]))

        if (index + 1) == len(box_groups):
            sort_line = self.sort_tuple(line)
            for word_num in range(len(sort_line)):
                align_text = align_text + ' ' + sort_line[word_num][0]

        return align_text.split()

    def run_inference_per_input(self, frame_path, bounding_box_dict, frame_height=config.frame_height,
                                frame_width=config.frame_width, batch_size=config.batch_size):
        frame_name = os.path.basename(frame_path)
        boxes = bounding_box_dict.get(frame_name, [])
        if not boxes:
            box_dict = bounding_box_dict.get(os.path.dirname(frame_path), {})
            boxes = box_dict.get(frame_name, [])
        image = self.read_img(frame_path, frame_height=frame_height, frame_width=frame_width)
        image = cv2.cvtColor(image, cv2.COLOR_RGB2GRAY)
        crops = []
        for box in boxes:
            crops.append(tools.warp_box(image=image, box=box, target_height=31, target_width=200))
        if not crops:
            return [{frame_path: []}]
        x = np.float32(crops) / 255
        if len(x.shape) == 3:
            x = x[..., np.newaxis]
        predictions = []
        for i in range(0, x.shape[0], batch_size):
            batch_x = x[i:i+batch_size]
            predict = run_optical_character_recognition_model(batch_x)
            batch_prediction = [
                ''.join([self._alphabet[idx] for idx in row if idx not in [self._blank_label_idx, -1]])
                for row in predict
            ]
            predictions.extend(batch_prediction)
        assert len(boxes) == len(predictions)
        aligned_text = self.apply_text_cleaning(self.align_text(np.array(boxes), predictions))
        return [{frame_path: aligned_text}]

    def run_optical_character_recognition(self, frames_list, bounding_box_info=None, compute_cores=1, synchronous=True,
                                          frame_width=config.frame_width, frame_height=config.frame_height,
                                          batch_size=config.batch_size, **kwargs):
        """ Performs optical character recognition on frames and returns
        a clean and processed recognized text.

        :param compute_cores:
        :param frames_list: A list of frames from a video.
        :param frame_height
        :param frame_width
        :param batch_size
        :param bounding_box_info: Json file consisting of bounding boxes for current scene folder.
        :param synchronous: (Optional) Default True for batching.
        """
        with open(bounding_box_info, 'r') as f:
            bounding_box_dict = json.load(f)
        result_list = self.run_inference(frames_list, synchronous, compute_cores, bounding_box_dict, frame_height,
                                         frame_width, batch_size)
        result = [frame_output for batch_output in result_list for frame_output in batch_output]
        return result
