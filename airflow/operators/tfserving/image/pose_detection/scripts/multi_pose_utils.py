import numpy as np
import scipy.ndimage as ndi


PART_NAMES = [
    "nose", "leftEye", "rightEye", "leftEar", "rightEar", "leftShoulder",
    "rightShoulder", "leftElbow", "rightElbow", "leftWrist", "rightWrist",
    "leftHip", "rightHip", "leftKnee", "rightKnee", "leftAnkle", "rightAnkle"
]

NUM_KEYPOINTS = len(PART_NAMES)

PART_IDS = {pn: pid for pid, pn in enumerate(PART_NAMES)}

CONNECTED_PART_NAMES = [
    ("leftHip", "leftShoulder"), ("leftElbow", "leftShoulder"),
    ("leftElbow", "leftWrist"), ("leftHip", "leftKnee"),
    ("leftKnee", "leftAnkle"), ("rightHip", "rightShoulder"),
    ("rightElbow", "rightShoulder"), ("rightElbow", "rightWrist"),
    ("rightHip", "rightKnee"), ("rightKnee", "rightAnkle"),
    ("leftShoulder", "rightShoulder"), ("leftHip", "rightHip")
]

CONNECTED_PART_INDICES = [(PART_IDS[a], PART_IDS[b]) for a, b in CONNECTED_PART_NAMES]

LOCAL_MAXIMUM_RADIUS = 1

POSE_CHAIN = [
    ("nose", "leftEye"), ("leftEye", "leftEar"), ("nose", "rightEye"),
    ("rightEye", "rightEar"), ("nose", "leftShoulder"),
    ("leftShoulder", "leftElbow"), ("leftElbow", "leftWrist"),
    ("leftShoulder", "leftHip"), ("leftHip", "leftKnee"),
    ("leftKnee", "leftAnkle"), ("nose", "rightShoulder"),
    ("rightShoulder", "rightElbow"), ("rightElbow", "rightWrist"),
    ("rightShoulder", "rightHip"), ("rightHip", "rightKnee"),
    ("rightKnee", "rightAnkle")
]

PARENT_CHILD_TUPLES = [(PART_IDS[parent], PART_IDS[child]) for parent, child in POSE_CHAIN]

PART_CHANNELS = [
  'left_face',
  'right_face',
  'right_upper_leg_front',
  'right_lower_leg_back',
  'right_upper_leg_back',
  'left_lower_leg_front',
  'left_upper_leg_front',
  'left_upper_leg_back',
  'left_lower_leg_back',
  'right_feet',
  'right_lower_leg_front',
  'left_feet',
  'torso_front',
  'torso_back',
  'right_upper_arm_front',
  'right_upper_arm_back',
  'right_lower_arm_back',
  'left_lower_arm_front',
  'left_upper_arm_front',
  'left_upper_arm_back',
  'left_lower_arm_back',
  'right_hand',
  'right_lower_arm_front',
  'left_hand'
]


def traverse_to_targ_keypoint(edge_id, source_keypoint, target_keypoint_id, scores, offsets, output_stride,
                              displacements):
    height = scores.shape[0]
    width = scores.shape[1]
    source_keypoint_indices = np.clip(
        np.round(source_keypoint / output_stride), a_min=0, a_max=[height - 1, width - 1]).astype(np.int32)
    displaced_point = source_keypoint + displacements[
        source_keypoint_indices[0], source_keypoint_indices[1], edge_id]

    displaced_point_indices = np.clip(
        np.round(displaced_point / output_stride), a_min=0, a_max=[height - 1, width - 1]).astype(np.int32)
    score = scores[displaced_point_indices[0], displaced_point_indices[1], target_keypoint_id]

    image_coord = displaced_point_indices * output_stride + offsets[
        displaced_point_indices[0], displaced_point_indices[1], target_keypoint_id]

    return score, image_coord


def decode_pose(root_score, root_id, root_image_coord, scores, offsets, output_stride, displacements_fwd,
                displacements_bwd):
    num_parts = scores.shape[2]
    num_edges = len(PARENT_CHILD_TUPLES)

    instance_keypoint_scores = np.zeros(num_parts)
    instance_keypoint_coords = np.zeros((num_parts, 2))
    instance_keypoint_scores[root_id] = root_score
    instance_keypoint_coords[root_id] = root_image_coord

    for edge in reversed(range(num_edges)):
        target_keypoint_id, source_keypoint_id = PARENT_CHILD_TUPLES[edge]
        if (instance_keypoint_scores[source_keypoint_id] > 0.0 and
                instance_keypoint_scores[target_keypoint_id] == 0.0):
            score, coords = traverse_to_targ_keypoint(
                edge,
                instance_keypoint_coords[source_keypoint_id],
                target_keypoint_id,
                scores, offsets, output_stride, displacements_bwd)
            instance_keypoint_scores[target_keypoint_id] = score
            instance_keypoint_coords[target_keypoint_id] = coords

    for edge in range(num_edges):
        source_keypoint_id, target_keypoint_id = PARENT_CHILD_TUPLES[edge]
        if (instance_keypoint_scores[source_keypoint_id] > 0.0 and
                instance_keypoint_scores[target_keypoint_id] == 0.0):
            score, coords = traverse_to_targ_keypoint(
                edge,
                instance_keypoint_coords[source_keypoint_id],
                target_keypoint_id,
                scores, offsets, output_stride, displacements_fwd)
            instance_keypoint_scores[target_keypoint_id] = score
            instance_keypoint_coords[target_keypoint_id] = coords

    return instance_keypoint_scores, instance_keypoint_coords


def within_nms_radius(poses, squared_nms_radius, point, keypoint_id):
    for _, _, pose_coord in poses:
        if np.sum((pose_coord[keypoint_id] - point) ** 2) <= squared_nms_radius:
            return True
    return False


def within_nms_radius_fast(pose_coords, squared_nms_radius, point):
    if not pose_coords.shape[0]:
        return False
    return np.any(np.sum((pose_coords - point) ** 2, axis=1) <= squared_nms_radius)


def get_instance_score(
        existing_poses, squared_nms_radius,
        keypoint_scores, keypoint_coords):
    not_overlapped_scores = 0.
    for keypoint_id in range(len(keypoint_scores)):
        if not within_nms_radius(
                existing_poses, squared_nms_radius,
                keypoint_coords[keypoint_id], keypoint_id):
            not_overlapped_scores += keypoint_scores[keypoint_id]
    return not_overlapped_scores / len(keypoint_scores)


def get_instance_score_fast(
        exist_pose_coords,
        squared_nms_radius,
        keypoint_scores, keypoint_coords):

    if exist_pose_coords.shape[0]:
        s = np.sum((exist_pose_coords - keypoint_coords) ** 2, axis=2) > squared_nms_radius
        not_overlapped_scores = np.sum(keypoint_scores[np.all(s, axis=0)])
    else:
        not_overlapped_scores = np.sum(keypoint_scores)
    return not_overlapped_scores / len(keypoint_scores)


def score_is_max_in_local_window(keypoint_id, score, hmy, hmx, local_max_radius, scores):
    height = scores.shape[0]
    width = scores.shape[1]

    y_start = max(hmy - local_max_radius, 0)
    y_end = min(hmy + local_max_radius + 1, height)
    x_start = max(hmx - local_max_radius, 0)
    x_end = min(hmx + local_max_radius + 1, width)

    for y in range(y_start, y_end):
        for x in range(x_start, x_end):
            if scores[y, x, keypoint_id] > score:
                return False
    return True


def build_part_with_score(score_threshold, local_max_radius, scores):
    parts = []
    height = scores.shape[0]
    width = scores.shape[1]
    num_keypoints = scores.shape[2]

    for hmy in range(height):
        for hmx in range(width):
            for keypoint_id in range(num_keypoints):
                score = scores[hmy, hmx, keypoint_id]
                if score < score_threshold:
                    continue
                if score_is_max_in_local_window(keypoint_id, score, hmy, hmx,
                                                local_max_radius, scores):
                    parts.append((
                        score, keypoint_id, np.array((hmy, hmx))
                    ))
    return parts


def build_part_with_score_fast(score_threshold, local_max_radius, scores):
    parts = []
    num_keypoints = scores.shape[2]
    lmd = 2 * local_max_radius + 1

    for keypoint_id in range(num_keypoints):
        kp_scores = scores[:, :, keypoint_id].copy()
        kp_scores[kp_scores < score_threshold] = 0.
        max_vals = ndi.maximum_filter(kp_scores, size=lmd, mode='constant')
        max_loc = np.logical_and(kp_scores == max_vals, kp_scores > 0)
        max_loc_idx = max_loc.nonzero()
        for y, x in zip(*max_loc_idx):
            parts.append((
                scores[y, x, keypoint_id],
                keypoint_id,
                np.array((y, x))
            ))

    return parts


def decode_multiple_poses(scores, offsets, displacements_fwd, displacements_bwd, output_stride, max_pose_detections=10,
                          score_threshold=0.5, nms_radius=20, min_pose_score=0., output_ratio=(1.0, 1.0)):
    scores, offsets, displacements_bwd, displacements_fwd = (
        np.squeeze(scores),
        np.squeeze(offsets),
        np.squeeze(displacements_bwd),
        np.squeeze(displacements_fwd)
    )

    pose_count = 0
    pose_scores = np.zeros(max_pose_detections)
    pose_keypoint_scores = np.zeros((max_pose_detections, NUM_KEYPOINTS))
    pose_keypoint_coords = np.zeros((max_pose_detections, NUM_KEYPOINTS, 2))

    squared_nms_radius = nms_radius ** 2

    scored_parts = build_part_with_score_fast(score_threshold, LOCAL_MAXIMUM_RADIUS, scores)
    scored_parts = sorted(scored_parts, key=lambda x: x[0], reverse=True)

    height = scores.shape[0]
    width = scores.shape[1]
    offsets = offsets.reshape((height, width, 2, -1)).swapaxes(2, 3)
    displacements_fwd = displacements_fwd.reshape((height, width, 2, -1)).swapaxes(2, 3)
    displacements_bwd = displacements_bwd.reshape((height, width, 2, -1)).swapaxes(2, 3)

    for root_score, root_id, root_coord in scored_parts:
        root_image_coords = root_coord * output_stride + offsets[
            root_coord[0], root_coord[1], root_id]

        if within_nms_radius_fast(
                pose_keypoint_coords[:pose_count, root_id, :], squared_nms_radius, root_image_coords):
            continue

        keypoint_scores, keypoint_coords = decode_pose(
            root_score, root_id, root_image_coords,
            scores, offsets, output_stride,
            displacements_fwd, displacements_bwd)
        pose_score = get_instance_score_fast(
            pose_keypoint_coords[:pose_count, :, :], squared_nms_radius, keypoint_scores, keypoint_coords)

        if min_pose_score == 0. or pose_score >= min_pose_score:
            pose_scores[pose_count] = pose_score
            pose_keypoint_scores[pose_count, :] = keypoint_scores
            pose_keypoint_coords[pose_count, :, :] = keypoint_coords
            pose_count += 1

        if pose_count >= max_pose_detections:
            break
    pose_scores, pose_keypoint_coords, pose_keypoint_scores = (
        pose_scores[:pose_count],
        pose_keypoint_coords[:pose_count, :, :],
        pose_keypoint_scores[:pose_count, :]
    )
    output_poses = []
    for current_pose_score, current_pose_coords, current_keypoint_scores in zip(pose_scores, pose_keypoint_coords,
                                                                                pose_keypoint_scores):
        keypoints = [{
            'position': {
                'y': position[0]*output_ratio[0],
                'x': position[1]*output_ratio[1]
            },
            'part': part_name,
            'score': score
        } for position, score, part_name in zip(current_pose_coords, current_keypoint_scores, PART_NAMES)]
        pose_dict = {
            'score': current_pose_score,
            'keypoints': keypoints
        }
        output_poses.append(pose_dict)
    return output_poses
