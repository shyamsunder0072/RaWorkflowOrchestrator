import numpy as np


PART_NAMES = [
    'nose', 'leftEye', 'rightEye', 'leftEar', 'rightEar', 'leftShoulder',
    'rightShoulder', 'leftElbow', 'rightElbow', 'leftWrist', 'rightWrist',
    'leftHip', 'rightHip', 'leftKnee', 'rightKnee', 'leftAnkle', 'rightAnkle'
]

NUM_KEYPOINTS = len(PART_NAMES)


def argmax2d(t):
    """Function to return Depthwise argmax for a 3D tensor.
    :param t: Input tensor of shape(H, W, D). Output will be of shape (D, 2) where each value is [y, x] for each D.
    """
    if len(t.shape) > 3:
        t = np.squeeze(t)
    if not len(t.shape) == 3:
        print("Input must be a 3D matrix, or be able to be squeezed into one.")
        return
    height, width, depth = t.shape

    reshaped_t = np.reshape(np.copy(t), [height * width, depth])
    argmax_coords = np.argmax(reshaped_t, axis=0)
    y_coords = argmax_coords // width
    x_coords = argmax_coords % width

    return np.concatenate([np.expand_dims(y_coords, 1), np.expand_dims(x_coords, 1)], axis=1)


def get_offset_vectors(heatmaps_coords, offsets):
    """Function to calculate offset vectors from heatmap co-ordinates and offsets tensor.
    :param heatmaps_coords: Co-ordinates obtained from heatmap.
    :param offsets: Offset vectors.
    """
    result = []

    for keypoint in range(NUM_KEYPOINTS):
        heatmap_y = heatmaps_coords[keypoint, 0]
        heatmap_x = heatmaps_coords[keypoint, 1]

        offset_y = offsets[heatmap_y, heatmap_x, keypoint]
        offset_x = offsets[heatmap_y, heatmap_x, keypoint + NUM_KEYPOINTS]

        result.append([offset_y, offset_x])

    return result


def get_offset_points(heatmaps_coords, offsets, output_stride):
    """Function to compute offset points from heatmap co-ordinates, offset vectors and output stride.
    :param heatmaps_coords: Co-ordinates computed from heatmaps.
    :param offsets: Offsets vectors.
    :param output_stride: Output stride of model.
    """
    offset_vectors = get_offset_vectors(heatmaps_coords, offsets)
    scaled_heatmap = heatmaps_coords * output_stride
    return scaled_heatmap + offset_vectors


def get_points_confidence(heatmaps, heatmaps_coords):
    """Function to compute key points score from heatmaps and heatmap co-ordinates
    :param heatmaps: Heatmaps for each Key point.
    :param heatmaps_coords: Co-ordinates of key points in heatmap.
    """
    result = []

    for keypoint in range(NUM_KEYPOINTS):
        result.append(heatmaps[heatmaps_coords[keypoint, 0], heatmaps_coords[keypoint, 1], keypoint])

    return result


def decode_single_pose(heatmaps, offsets, output_stride, output_ratio=(1.0, 1.0)):
    """Function to decode single pose from heatmaps, offset vectors and model output stride.
    :param heatmaps: Heat map for each key point.
    :param offsets: Offset vectors.
    :param output_stride: Output stride of the model
    :param output_ratio: Ratio to extrapolate keypoints.
    """
    heatmaps = np.squeeze(heatmaps)
    offsets = np.squeeze(offsets)

    heatmaps_coords = argmax2d(heatmaps)
    offset_points = get_offset_points(heatmaps_coords, offsets, output_stride)
    keypoint_confidence = get_points_confidence(heatmaps, heatmaps_coords)

    keypoints = [{
        "position": {
            "y": offset_points[keypoint, 0]*output_ratio[0],
            "x": offset_points[keypoint, 1]*output_ratio[1]
        },
        "part": PART_NAMES[keypoint],
        "score": score
    } for keypoint, score in enumerate(keypoint_confidence)]

    return {
        "keypoints": keypoints,
        "score": (sum(keypoint_confidence) / len(keypoint_confidence))
    }
