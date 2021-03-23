from airflow.operators.tfserving.image.face_key_points_detection.scripts.face_key_points_detection import \
    FaceKeyPointsDetection
run_face_key_points_detection = FaceKeyPointsDetection().run_face_key_points_detection
