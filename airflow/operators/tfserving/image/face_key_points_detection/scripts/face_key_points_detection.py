#!/usr/bin/python3
# -*- coding: utf-8 -*-

import cv2
import os
import numpy as np

from airflow.operators.tfserving.base_classes.base_module import BaseModule
from airflow.operators.tfserving.image.face_key_points_detection.models.face_key_points_detection_model.\
    face_keypoints_detection_model import FaceKeyPointsDetectionModel
from airflow.operators.tfserving.image.face_key_points_detection.scripts import conf as params
from airflow.operators.tfserving.image.face_key_points_detection.scripts.utils import StageStatus, align_face


class FaceKeyPointsDetection(BaseModule):

    def __init__(self):
        self._min_face_size = params.min_face_size
        self._steps_threshold = params.steps_threshold
        self._scale_factor = params.scale_factor
        self._p_net = FaceKeyPointsDetectionModel('p_net')
        self._r_net = FaceKeyPointsDetectionModel('r_net')
        self._o_net = FaceKeyPointsDetectionModel('o_net')
        super().__init__()

    def __compute_scale_pyramid(self, m, min_layer):
        scales = []
        factor_count = 0

        while min_layer >= 12:
            scales += [m * np.power(self._scale_factor, factor_count)]
            min_layer = min_layer * self._scale_factor
            factor_count += 1

        return scales

    @staticmethod
    def __scale_image(image, scale):

        height, width, _ = image.shape

        width_scaled = int(np.ceil(width * scale))
        height_scaled = int(np.ceil(height * scale))

        im_data = cv2.resize(image, (width_scaled, height_scaled), interpolation=cv2.INTER_AREA)

        # Normalize the image's pixels
        im_data_normalized = (im_data - 127.5) * 0.0078125

        return im_data_normalized

    @staticmethod
    def __generate_bounding_box(imap, reg, scale, t):
        stride = 2
        cell_size = 12

        imap = np.transpose(imap)
        dx1 = np.transpose(reg[:, :, 0])
        dy1 = np.transpose(reg[:, :, 1])
        dx2 = np.transpose(reg[:, :, 2])
        dy2 = np.transpose(reg[:, :, 3])

        y, x = np.where(imap >= t)

        if y.shape[0] == 1:
            dx1 = np.flipud(dx1)
            dy1 = np.flipud(dy1)
            dx2 = np.flipud(dx2)
            dy2 = np.flipud(dy2)

        score = imap[(y, x)]
        reg = np.transpose(np.vstack([dx1[(y, x)], dy1[(y, x)], dx2[(y, x)], dy2[(y, x)]]))

        if reg.size == 0:
            reg = np.empty(shape=(0, 3))

        bb = np.transpose(np.vstack([y, x]))

        q1 = np.fix((stride * bb + 1) / scale)
        q2 = np.fix((stride * bb + cell_size) / scale)
        bounding_box = np.hstack([q1, q2, np.expand_dims(score, 1), reg])

        return bounding_box, reg

    @staticmethod
    def __nms(boxes, threshold, method):
        if boxes.size == 0:
            return np.empty((0, 3))

        x1 = boxes[:, 0]
        y1 = boxes[:, 1]
        x2 = boxes[:, 2]
        y2 = boxes[:, 3]
        s = boxes[:, 4]

        area = (x2 - x1 + 1) * (y2 - y1 + 1)
        sorted_s = np.argsort(s)

        pick = np.zeros_like(s, dtype=np.int16)
        counter = 0
        while sorted_s.size > 0:
            i = sorted_s[-1]
            pick[counter] = i
            counter += 1
            idx = sorted_s[0:-1]

            xx1 = np.maximum(x1[i], x1[idx])
            yy1 = np.maximum(y1[i], y1[idx])
            xx2 = np.minimum(x2[i], x2[idx])
            yy2 = np.minimum(y2[i], y2[idx])

            w = np.maximum(0.0, xx2 - xx1 + 1)
            h = np.maximum(0.0, yy2 - yy1 + 1)

            inter = w * h

            if method is 'Min':
                o = inter / np.minimum(area[i], area[idx])
            else:
                o = inter / (area[i] + area[idx] - inter)

            sorted_s = sorted_s[np.where(o <= threshold)]

        pick = pick[0:counter]

        return pick

    @staticmethod
    def __pad(total_boxes, w, h):
        tmp_w = (total_boxes[:, 2] - total_boxes[:, 0] + 1).astype(np.int32)
        tmp_h = (total_boxes[:, 3] - total_boxes[:, 1] + 1).astype(np.int32)
        num_box = total_boxes.shape[0]

        dx = np.ones(num_box, dtype=np.int32)
        dy = np.ones(num_box, dtype=np.int32)
        edx = tmp_w.copy().astype(np.int32)
        edy = tmp_h.copy().astype(np.int32)

        x = total_boxes[:, 0].copy().astype(np.int32)
        y = total_boxes[:, 1].copy().astype(np.int32)
        ex = total_boxes[:, 2].copy().astype(np.int32)
        ey = total_boxes[:, 3].copy().astype(np.int32)

        tmp = np.where(ex > w)
        edx.flat[tmp] = np.expand_dims(-ex[tmp] + w + tmp_w[tmp], 1)
        ex[tmp] = w

        tmp = np.where(ey > h)
        edy.flat[tmp] = np.expand_dims(-ey[tmp] + h + tmp_h[tmp], 1)
        ey[tmp] = h

        tmp = np.where(x < 1)
        dx.flat[tmp] = np.expand_dims(2 - x[tmp], 1)
        x[tmp] = 1

        tmp = np.where(y < 1)
        dy.flat[tmp] = np.expand_dims(2 - y[tmp], 1)
        y[tmp] = 1

        return dy, edy, dx, edx, y, ey, x, ex, tmp_w, tmp_h

    @staticmethod
    def __re_rec(bbox):
        # convert bbox to square
        height = bbox[:, 3] - bbox[:, 1]
        width = bbox[:, 2] - bbox[:, 0]
        max_side_length = np.maximum(width, height)
        bbox[:, 0] = bbox[:, 0] + width * 0.5 - max_side_length * 0.5
        bbox[:, 1] = bbox[:, 1] + height * 0.5 - max_side_length * 0.5
        bbox[:, 2:4] = bbox[:, 0:2] + np.transpose(np.tile(max_side_length, (2, 1)))
        return bbox

    @staticmethod
    def __bb_reg(bounding_box, reg):
        # calibrate bounding boxes
        if reg.shape[1] == 1:
            reg = np.reshape(reg, (reg.shape[2], reg.shape[3]))

        w = bounding_box[:, 2] - bounding_box[:, 0] + 1
        h = bounding_box[:, 3] - bounding_box[:, 1] + 1
        b1 = bounding_box[:, 0] + reg[:, 0] * w
        b2 = bounding_box[:, 1] + reg[:, 1] * h
        b3 = bounding_box[:, 2] + reg[:, 2] * w
        b4 = bounding_box[:, 3] + reg[:, 3] * h
        bounding_box[:, 0:4] = np.transpose(np.vstack([b1, b2, b3, b4]))
        return bounding_box

    def __p_net_stage(self, image, scales, stage_status):
        total_boxes = np.empty((0, 9))
        status = stage_status

        for scale in scales:
            scaled_image = self.__scale_image(image, scale)

            img_x = np.expand_dims(scaled_image, 0)
            img_y = np.transpose(img_x, (0, 2, 1, 3))

            out = self._p_net.run(img_y)

            out0 = np.transpose(out[0], (0, 2, 1, 3))
            out1 = np.transpose(out[1], (0, 2, 1, 3))

            boxes, _ = self.__generate_bounding_box(out1[0, :, :, 1].copy(),
                                                    out0[0, :, :, :].copy(), scale, self._steps_threshold[0])

            # inter-scale nms
            pick = self.__nms(boxes.copy(), 0.5, 'Union')
            if boxes.size > 0 and pick.size > 0:
                boxes = boxes[pick, :]
                total_boxes = np.append(total_boxes, boxes, axis=0)

        num_boxes = total_boxes.shape[0]

        if num_boxes > 0:
            pick = self.__nms(total_boxes.copy(), 0.7, 'Union')
            total_boxes = total_boxes[pick, :]

            reg_w = total_boxes[:, 2] - total_boxes[:, 0]
            reg_h = total_boxes[:, 3] - total_boxes[:, 1]

            qq1 = total_boxes[:, 0] + total_boxes[:, 5] * reg_w
            qq2 = total_boxes[:, 1] + total_boxes[:, 6] * reg_h
            qq3 = total_boxes[:, 2] + total_boxes[:, 7] * reg_w
            qq4 = total_boxes[:, 3] + total_boxes[:, 8] * reg_h

            total_boxes = np.transpose(np.vstack([qq1, qq2, qq3, qq4, total_boxes[:, 4]]))
            total_boxes = self.__re_rec(total_boxes.copy())

            total_boxes[:, 0:4] = np.fix(total_boxes[:, 0:4]).astype(np.int32)
            status = StageStatus(self.__pad(total_boxes.copy(), stage_status.width, stage_status.height),
                                 width=stage_status.width, height=stage_status.height)

        return total_boxes, status

    def __r_net_stage(self, img, total_boxes, stage_status):

        num_boxes = total_boxes.shape[0]
        if num_boxes == 0:
            return total_boxes, stage_status

        # second stage
        temp_img = np.zeros(shape=(24, 24, 3, num_boxes))

        for k in range(0, num_boxes):
            tmp = np.zeros((int(stage_status.tmp_h[k]), int(stage_status.tmp_w[k]), 3))

            tmp[stage_status.dy[k] - 1:stage_status.edy[k], stage_status.dx[k] - 1:stage_status.edx[k], :] = \
                img[stage_status.y[k] - 1:stage_status.ey[k], stage_status.x[k] - 1:stage_status.ex[k], :]

            if tmp.shape[0] > 0 and tmp.shape[1] > 0 or tmp.shape[0] == 0 and tmp.shape[1] == 0:
                temp_img[:, :, :, k] = cv2.resize(tmp, (24, 24), interpolation=cv2.INTER_AREA)

            else:
                return np.empty(shape=(0,)), stage_status

        temp_img = (temp_img - 127.5) * 0.0078125
        temp_img1 = np.transpose(temp_img, (3, 1, 0, 2))

        out = self._r_net.run(temp_img1)

        out0 = np.transpose(out[0])
        out1 = np.transpose(out[1])

        score = out1[1, :]

        i_pass = np.where(score > self._steps_threshold[1])

        total_boxes = np.hstack([total_boxes[i_pass[0], 0:4].copy(), np.expand_dims(score[i_pass].copy(), 1)])

        mv = out0[:, i_pass[0]]

        if total_boxes.shape[0] > 0:
            pick = self.__nms(total_boxes, 0.7, 'Union')
            total_boxes = total_boxes[pick, :]
            total_boxes = self.__bb_reg(total_boxes.copy(), np.transpose(mv[:, pick]))
            total_boxes = self.__re_rec(total_boxes.copy())

        return total_boxes, stage_status

    def __o_net_stage(self, img, total_boxes, stage_status):

        num_boxes = total_boxes.shape[0]
        if num_boxes == 0:
            return total_boxes, np.empty(shape=(0,))

        total_boxes = np.fix(total_boxes).astype(np.int32)

        status = StageStatus(self.__pad(total_boxes.copy(), stage_status.width, stage_status.height),
                             width=stage_status.width, height=stage_status.height)

        temp_img = np.zeros((48, 48, 3, num_boxes))

        for k in range(0, num_boxes):

            tmp = np.zeros((int(status.tmp_h[k]), int(status.tmp_w[k]), 3))

            tmp[status.dy[k] - 1:status.edy[k], status.dx[k] - 1:status.edx[k], :] = \
                img[status.y[k] - 1:status.ey[k], status.x[k] - 1:status.ex[k], :]

            if tmp.shape[0] > 0 and tmp.shape[1] > 0 or tmp.shape[0] == 0 and tmp.shape[1] == 0:
                temp_img[:, :, :, k] = cv2.resize(tmp, (48, 48), interpolation=cv2.INTER_AREA)
            else:
                return np.empty(shape=(0,)), np.empty(shape=(0,))

        temp_img = (temp_img - 127.5) * 0.0078125
        temp_img1 = np.transpose(temp_img, (3, 1, 0, 2))

        out = self._o_net.run(temp_img1)
        out0 = np.transpose(out[0])
        out1 = np.transpose(out[1])
        out2 = np.transpose(out[2])

        score = out2[1, :]

        points = out1

        i_pass = np.where(score > self._steps_threshold[2])

        points = points[:, i_pass[0]]

        total_boxes = np.hstack([total_boxes[i_pass[0], 0:4].copy(), np.expand_dims(score[i_pass].copy(), 1)])

        mv = out0[:, i_pass[0]]

        w = total_boxes[:, 2] - total_boxes[:, 0] + 1
        h = total_boxes[:, 3] - total_boxes[:, 1] + 1

        points[0:5, :] = np.tile(w, (5, 1)) * points[0:5, :] + np.tile(total_boxes[:, 0], (5, 1)) - 1
        points[5:10, :] = np.tile(h, (5, 1)) * points[5:10, :] + np.tile(total_boxes[:, 1], (5, 1)) - 1

        if total_boxes.shape[0] > 0:
            total_boxes = self.__bb_reg(total_boxes.copy(), np.transpose(mv))
            pick = self.__nms(total_boxes.copy(), 0.7, 'Min')
            total_boxes = total_boxes[pick, :]
            points = points[:, pick]

        return total_boxes, points

    def run_inference_per_input(self, image_path, confidence_threshold, dump_aligned_images=False,
                                output_dir=None):
        img = cv2.cvtColor(cv2.imread(image_path), cv2.COLOR_BGR2RGB)
        height, width, _ = img.shape
        stage_status = StageStatus(width=width, height=height)

        m = 12 / self._min_face_size
        min_layer = np.amin([height, width]) * m

        scales = self.__compute_scale_pyramid(m, min_layer)

        result = [scales, stage_status]
        result = self.__p_net_stage(img, result[0], result[1])
        result = self.__r_net_stage(img, result[0], result[1])
        total_boxes, points = self.__o_net_stage(img, result[0], result[1])

        bounding_boxes = []
        flag = True
        for bounding_box, key_points in zip(total_boxes, points.T):
            x = max(0, int(bounding_box[0]))
            y = max(0, int(bounding_box[1]))
            width = int(bounding_box[2] - x)
            height = int(bounding_box[3] - y)
            if bounding_box[-1] >= confidence_threshold:
                if flag and dump_aligned_images:
                    flag = False
                    cropped_image = img[int(y):int(y+height), int(x):int(x+width)]
                    left_eye = (int(key_points[0]), int(key_points[5]))
                    left_eye = max(left_eye[0] - x, 0), max(left_eye[1] - y, 0)
                    right_eye = (int(key_points[1]), int(key_points[6]))
                    right_eye = max(right_eye[0] - x, 0), max(right_eye[1] - y, 0)
                    aligned_face = align_face(cropped_image, left_eye, right_eye)
                    basename, ext = os.path.splitext(os.path.basename(image_path))
                    if output_dir is None:
                        dir_name = os.path.dirname(image_path)
                        output_dir = os.path.join(dir_name, basename)
                    else:
                        output_dir = output_dir.format(basename=basename)
                    if not os.path.exists(output_dir):
                        os.makedirs(output_dir, exist_ok=True)
                    output_image_path = os.path.join(output_dir, basename+'_aligned'+ext)
                    cv2.imwrite(output_image_path, cv2.cvtColor(aligned_face, cv2.COLOR_RGB2BGR))
                bounding_boxes.append({
                    'box': {
                        'x': x,
                        'y': y,
                        'width': width,
                        'height': height
                    },
                    'confidence': bounding_box[-1],
                    'key_points': {
                        'left_eye': (int(key_points[0]), int(key_points[5])),
                        'right_eye': (int(key_points[1]), int(key_points[6])),
                        'nose': (int(key_points[2]), int(key_points[7])),
                        'mouth_left': (int(key_points[3]), int(key_points[8])),
                        'mouth_right': (int(key_points[4]), int(key_points[9])),
                    }
                })

        return {image_path: bounding_boxes}

    def run_face_key_points_detection(self, images_list, confidence_threshold=params.default_confidence_threshold,
                                      dump_aligned_images=False, aligned_output_dir=None, compute_cores=1,
                                      synchronous=False, **kwargs):
        """ Performs face key points detection .
        :param compute_cores:
        :param confidence_threshold
        :param images_list: A list of frames from a video.
        :param dump_aligned_images
        :param aligned_output_dir
        :param synchronous: (Optional) Default True for batching.
        """
        if aligned_output_dir is None:
            return self.run_inference(images_list, synchronous, compute_cores, confidence_threshold,
                                      dump_aligned_images)
        else:
            return self.run_inference(images_list, synchronous, compute_cores, confidence_threshold,
                                      dump_aligned_images, aligned_output_dir)
