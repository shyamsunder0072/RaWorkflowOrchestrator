from airflow.operators.tfserving.image.optical_character_recognition.models.version1.optical_character_recognition_model import\
    OpticalCharacterRecognitionModel

run_optical_character_recognition_model = OpticalCharacterRecognitionModel().run
