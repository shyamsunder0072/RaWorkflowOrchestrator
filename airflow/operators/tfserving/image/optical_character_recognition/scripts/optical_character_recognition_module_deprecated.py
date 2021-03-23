#!/usr/bin/env python
# coding: utf8

"""
    Optical Character Recognition.

    Returns the top K ocr tags prediction using extracted frames of videos.
    It is based on open-sourced google tesseract engine.

"""

import gc
import multiprocessing
import os
import re
import uuid

import cv2
import nltk
import pytesseract
from PIL import Image

from airflow.operators.tfserving.image.optical_character_recognition.scripts import conf as config


class OCRModule(object):
    """A wrapper class for performing optical character recognition. """

    _CORPUS = set(nltk.corpus.words.words())
    _STOPWORDS = nltk.corpus.stopwords.words('english')
    _LEMMATIZER = nltk.stem.wordnet.WordNetLemmatizer()

    def __init__(self):
        """ Default constructor."""
        self._cores_per_task = config.cores_per_task
        return

    @staticmethod
    def apply_median_blur(bgr):
        """ Applies median blurring to remove noise. """
        gray = cv2.cvtColor(bgr, cv2.COLOR_BGR2GRAY)
        gray = cv2.medianBlur(gray, 3)
        return gray

    @staticmethod
    def apply_thresholding(bgr):
        """ Applies thresholding (binarization) to remove noise. """
        gray = cv2.cvtColor(bgr, cv2.COLOR_BGR2GRAY)
        gray = cv2.threshold(gray, 0, 255,
                             cv2.THRESH_BINARY | cv2.THRESH_OTSU)[1]
        return gray

    @staticmethod
    def apply_illumination_correction(bgr):
        """ Applies illumination correction only on L component. """
        lab = cv2.cvtColor(bgr, cv2.COLOR_BGR2LAB)
        lab_planes = cv2.split(lab)
        clahe = cv2.createCLAHE(clipLimit=2.0, tileGridSize=(8, 8))
        lab_planes[0] = clahe.apply(lab_planes[0])
        lab = cv2.merge(lab_planes)
        bgr = cv2.cvtColor(lab, cv2.COLOR_LAB2BGR)
        return bgr

    def preprocess_frame(self, bgr):
        # apply preprocessing techniques
        bgr = self.apply_illumination_correction(bgr)
        """ Applies illumination correction only on L component. """
        lab = cv2.cvtColor(bgr, cv2.COLOR_BGR2LAB)
        lab_planes = cv2.split(lab)
        clahe = cv2.createCLAHE(clipLimit=2.0, tileGridSize=(8, 8))
        lab_planes[0] = clahe.apply(lab_planes[0])
        lab = cv2.merge(lab_planes)
        bgr = cv2.cvtColor(lab, cv2.COLOR_LAB2BGR)
        # gray = self.apply_median_blur(bgr)
        """ Applies median blurring to remove noise. """
        gray = cv2.cvtColor(bgr, cv2.COLOR_BGR2GRAY)
        gray = cv2.medianBlur(gray, 3)
        return gray

    def apply_text_cleaning(self, video_text):
        """ Performs post processing on video texts.

        :param video_text:  List of raw video level texts.
        :return:            List of processed video level texts.
        """
        v = video_text.split(" ")
        v = [re.sub("[^a-zA-Z]", '', text) for text in v]
        v = [el for el in v if len(el) > 2]
        v = [el.lower() for el in v]
        v = [el for el in v if el not in self._STOPWORDS]
        v = [self._LEMMATIZER.lemmatize(el) for el in v]
        return v

    def recognize_texts_from_frames(self, frame):
        """ Performs text recognition on video frame.

        :param frame:   Path to a single raw video frame.
        :return         Target text as a result.
        """
        bgr = cv2.imread(frame)
        filename = "{}.png".format(uuid.uuid4().hex)
        cv2.imwrite(filename, bgr)

        # load the image as a PIL/Pillow image, apply OCR, and then delete
        # the temporary file
        frame_text = pytesseract.image_to_string(Image.open(filename), lang='eng', config='--psm 6')
        os.remove(filename)
        frame_text = self.apply_text_cleaning(frame_text)
        result = {frame: frame_text}
        return result

    def run_ocr(self, frames_list, compute_cores, synchronous=True):
        """ Performs optical character recognition on frames and returns
        a clean and processed recognized text.

        :param compute_cores:
        :param frames_list: A list of frames from a video.
        :param synchronous: (Optional) Default True for batching.
        """
        if synchronous:
            # using mp executor
            with multiprocessing.Pool(processes=min(compute_cores, len(frames_list)),
                                      maxtasksperchild=1000) as multi_inputs_pool:
                result = multi_inputs_pool.map(self.recognize_texts_from_frames, frames_list)
            gc.collect()
            # using concurrent executor
            # with ProcessPoolExecutor(self._cores_per_task) as multi_inputs_pool:
            #     result = multi_inputs_pool.map(self.recognize_texts_from_frames, frames_list)
        else:
            result = []
            for frame in frames_list:
                result.append(self.recognize_texts_from_frames(frame))
        return result
