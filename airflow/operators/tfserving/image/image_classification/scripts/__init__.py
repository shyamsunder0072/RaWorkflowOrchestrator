from airflow.operators.tfserving.image.image_classification.scripts.image_classification import ImageClassification


run_image_classification = ImageClassification().run_image_classification
