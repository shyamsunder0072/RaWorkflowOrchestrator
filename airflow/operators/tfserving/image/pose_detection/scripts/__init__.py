from airflow.operators.tfserving.image.pose_detection.scripts.pose_detection import PoseDetection
run_pose_detection = PoseDetection().run_pose_detection
