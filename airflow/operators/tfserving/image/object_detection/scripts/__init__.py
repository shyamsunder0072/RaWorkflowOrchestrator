from airflow.operators.tfserving.image.object_detection.scripts.object_detection import ObjectDetection

run_object_detection = ObjectDetection().run_object_detection
