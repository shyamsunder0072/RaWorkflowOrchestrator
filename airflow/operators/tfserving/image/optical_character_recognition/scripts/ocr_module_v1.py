#!/usr/bin/env python
# coding: utf8

"""
    Optical Character Recognition.

    Returns the top K ocr tags prediction using extracted frames of videos.

"""

import gc
import multiprocessing
import re
import string

import cv2
import nltk
import numpy as np

from airflow.operators.tfserving.image.optical_character_recognition.models import \
    run_detection_model, run_recognition_model
from airflow.operators.tfserving.image.optical_character_recognition.scripts import conf as config
from airflow.operators.tfserving.image.optical_character_recognition.scripts import tools


class OCRModule(object):
    """A wrapper class for performing optical character recognition. """

    _CORPUS = set(nltk.corpus.words.words())
    _STOPWORDS = nltk.corpus.stopwords.words('english')
    _LEMMATIZER = nltk.stem.wordnet.WordNetLemmatizer()

    def __init__(self):
        """ Default constructor."""
        self._cores_per_task = config.cores_per_task
        self._batch_size = config.batch_size
        self._alphabet = string.digits + string.ascii_lowercase
        self._blank_label_idx = len(self._alphabet)
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

    def recognize_texts_from_frames(self, batch_frames, frame_height=config.frame_height,
                                    frame_width=config.frame_width):
        """ Performs text recognition on video frame.

        :param batch_frames:  Path to batch of raw video frames.
        :param frame_height: Target height of each frame.
        :param frame_width: Target width of each frame.
        :return         Target text as a result.
        """
        images = np.array([self.preprocess_image(self.read_img(i, frame_height=frame_height, frame_width=frame_width))
                           for i in batch_frames], dtype=np.float32)
        images2 = np.array([self.read_img(i, frame_height=frame_height, frame_width=frame_width) for i in batch_frames],
                           dtype=np.float32)
        box_groups = self.get_boxes(run_detection_model(images))
        crops = []
        start_end = []
        for image, boxes in zip(images2, box_groups):
            image = cv2.cvtColor(image, code=cv2.COLOR_RGB2GRAY)
            for box in boxes:
                crops.append(
                    tools.warp_box(image=image, box=box, target_height=31, target_width=200))
            start = 0 if not start_end else start_end[-1][1]
            start_end.append((start, start + len(boxes)))
        if not crops:
            return [{frame: []} for frame in batch_frames]
        x = np.float32(crops) / 255
        if len(x.shape) == 3:
            x = x[..., np.newaxis]
        predict = run_recognition_model(x)
        predictions = [
            ''.join([self._alphabet[idx] for idx in row if idx not in [self._blank_label_idx, -1]])
            for row in predict
        ]
        predictions = [predictions[start:end] for start, end in start_end]
        aligned_text = []
        for box_group, pred in zip(box_groups, predictions):
            assert box_group.shape[0] == len(pred)
            aligned_text.append(self.apply_text_cleaning(self.align_text(box_group, pred)))
        return [{img: pred}for img, pred in zip(batch_frames, aligned_text)]

    def run_ocr(self, frames_list, compute_cores, synchronous=True):
        """ Performs optical character recognition on frames and returns
        a clean and processed recognized text.

        :param compute_cores:
        :param frames_list: A list of frames from a video.
        :param synchronous: (Optional) Default True for batching.
        """
        batch_argument_list = [frames_list[i:(i+self._batch_size)]
                               for i in range(0, len(frames_list), self._batch_size)]
        if synchronous:
            # using mp executor
            with multiprocessing.Pool(processes=min(compute_cores, len(batch_argument_list)),
                                      maxtasksperchild=1000) as multi_inputs_pool:
                result = multi_inputs_pool.map(self.recognize_texts_from_frames, batch_argument_list)
            result = [frame_output for batch_output in result for frame_output in batch_output]
            gc.collect()
        else:
            result = []
            for batch_frames in batch_argument_list:
                result.extend(self.recognize_texts_from_frames(batch_frames))
        return result
