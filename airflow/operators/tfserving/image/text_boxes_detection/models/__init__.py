from airflow.operators.tfserving.image.text_boxes_detection.models.version1.text_boxes_detection_model import \
    TextBoxesDetectionModel

run_text_boxes_detection_model = TextBoxesDetectionModel().run
