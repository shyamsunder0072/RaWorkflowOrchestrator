import cv2
import os
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3'
import numpy as np
import re
from tensorflow.keras.layers import MaxPooling2D

from airflow.operators.tfserving.base_classes.base_module import BaseModule


class FrameTextReduction(BaseModule):
    def __init__(self):
        super().__init__()

    @staticmethod
    def get_frame_num(frame):
        res = re.findall(r'\d+$', os.path.splitext(os.path.basename(frame))[0])
        if res:
            return int(res[-1])
        return frame

    @staticmethod
    def get_image_diff(prev_image_name, next_image_name, bw_threshold, zero_diff_threshold, max_pool_2d):
        prev_image = cv2.imread(prev_image_name, 0)
        next_image = cv2.imread(next_image_name, 0)
        # prev_gray = cv2.resize(prev_gray, (640, 360), cv2.INTER_AREA)
        # next_gray = cv2.resize(next_gray, (640, 360), cv2.INTER_AREA)
        if np.mean(next_image) > bw_threshold:
            diff = cv2.subtract(prev_image, next_image)
        else:
            diff = cv2.subtract(next_image, prev_image)
        diff_tf = diff.reshape((1, diff.shape[0], diff.shape[1], 1))
        max_grid = max_pool_2d(diff_tf).numpy()[0]
        max_grid = max_grid.reshape((diff.shape[0], diff.shape[1]))
        mask = (max_grid <= zero_diff_threshold)
        image_diff = np.copy(next_image)
        image_diff[mask] = diff[mask]
        return image_diff

    def run_inference_per_input(self, frames, output_scene_dir):
        bw_threshold = 150
        zero_diff_threshold = 20
        nbd_pixel_threshold = 10
        max_pool_2d = MaxPooling2D(pool_size=(nbd_pixel_threshold, nbd_pixel_threshold), strides=(1, 1), padding='same')
        image_list = sorted(frames, key=self.get_frame_num)
        if not os.path.exists(output_scene_dir):
            os.makedirs(output_scene_dir, exist_ok=True)
        if image_list:
            prev_image_name = image_list[0]
            prev_image = cv2.imread(prev_image_name, 0)
            cv2.imwrite(os.path.join(output_scene_dir, os.path.basename(prev_image_name)), prev_image)
            for image_num in range(1, len(image_list)):
                next_image_name = image_list[image_num]
                diff_img = self.get_image_diff(prev_image_name, next_image_name, bw_threshold, zero_diff_threshold,
                                               max_pool_2d)
                cv2.imwrite(os.path.join(output_scene_dir, os.path.basename(next_image_name)), diff_img)
                prev_image_name = next_image_name

    def run_frame_text_reduction(self, frames_list, output_dir=None, **kwargs):
        self.run_inference([frames_list], False, 1, output_dir)
