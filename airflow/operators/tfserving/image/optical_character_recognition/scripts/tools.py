import cv2
import numpy as np
from shapely import geometry
from scipy import spatial


def get_rotated_width_height(box):
    """
    Returns the width and height of a rotated rectangle

    Args:
        box: A list of four points starting in the top left
        corner and moving clockwise.
    """
    w = (spatial.distance.cdist(box[0][np.newaxis], box[1][np.newaxis], "euclidean") +
         spatial.distance.cdist(box[2][np.newaxis], box[3][np.newaxis], "euclidean")) / 2
    h = (spatial.distance.cdist(box[0][np.newaxis], box[3][np.newaxis], "euclidean") +
         spatial.distance.cdist(box[1][np.newaxis], box[2][np.newaxis], "euclidean")) / 2
    return int(w[0][0]), int(h[0][0])


def warp_box(image, box, target_height=None, target_width=None, margin=0, cval=None, return_transform=False,
             skip_rotate=False):
    """Warp a boxed region in an image given by a set of four points into
    a rectangle with a specified width and height. Useful for taking crops
    of distorted or rotated text.

    Args:
        image: The image from which to take the box
        box: A list of four points starting in the top left
            corner and moving clockwise.
        target_height: The height of the output rectangle.
        target_width: The width of the output rectangle.
        margin: The margin value used in perspective transformation.
        cval: The centre value.
        return_transform: Whether to return the transformation
            matrix with the image.
        skip_rotate: Boolean value whether to get rotated box.
    """
    if cval is None:
        cval = (0, 0, 0) if len(image.shape) == 3 else 0
    if not skip_rotate:
        box, _ = get_rotated_box(box)
    w, h = get_rotated_width_height(box)
    assert (
        (target_width is None and target_height is None)
        or (target_width is not None and target_height is not None)), \
        'Either both or neither of target width and height must be provided.'
    if target_width is None and target_height is None:
        target_width = w
        target_height = h
    scale = min(target_width / w, target_height / h)
    m = cv2.getPerspectiveTransform(src=box,
                                    dst=np.array([[margin, margin], [scale * w - margin, margin],
                                                  [scale * w - margin, scale * h - margin],
                                                  [margin, scale * h - margin]]).astype('float32'))
    crop = cv2.warpPerspective(image, m, dsize=(int(scale * w), int(scale * h)))
    target_shape = (target_height, target_width, 3) if len(image.shape) == 3 else (target_height,
                                                                                   target_width)
    full = (np.zeros(target_shape) + cval).astype('uint8')
    full[:crop.shape[0], :crop.shape[1]] = crop
    if return_transform:
        return full, m
    return full


def get_rotated_box(points):
    """Obtain the parameters of a rotated box.

    Returns:
        The vertices of the rotated box in top-left,
        top-right, bottom-right, bottom-left order along
        with the angle of rotation about the bottom left corner.
    """
    try:
        mp = geometry.MultiPoint(points=points)
        pts = np.array(list(zip(*mp.minimum_rotated_rectangle.exterior.xy)))[:-1]
    except AttributeError:
        pts = points

    x_sorted = pts[np.argsort(pts[:, 0]), :]

    left_most = x_sorted[:2, :]
    right_most = x_sorted[2:, :]

    left_most = left_most[np.argsort(left_most[:, 1]), :]
    (tl, bl) = left_most

    dist = spatial.distance.cdist(tl[np.newaxis], right_most, "euclidean")[0]
    (br, tr) = right_most[np.argsort(dist)[::-1], :]

    pts = np.array([tl, tr, br, bl], dtype="float32")

    rotation = np.arctan((tl[0] - bl[0]) / (tl[1] - bl[1]))
    return pts, rotation
