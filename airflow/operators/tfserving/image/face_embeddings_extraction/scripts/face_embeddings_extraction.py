#!/usr/bin/env python
# coding: utf8

from airflow.operators.tfserving.base_classes.base_module import BaseModule
from airflow.operators.tfserving.image.face_embeddings_extraction.models.face_embeddings_extraction_model.\
    face_embeddings_extraction_model import FaceEmbeddingsExtractionModel
from airflow.operators.tfserving.image.face_embeddings_extraction.scripts import conf as params


class FaceEmbeddingsExtraction(BaseModule):

    def __init__(self):
        super().__init__()

    def run_inference_per_input(self, image, face_embeddings_model):
        embedding = face_embeddings_model.run(image)
        return {image: embedding}

    def run_face_embeddings_extraction(self, images_list, model_name=params.default_model, compute_cores=1,
                                       synchronous=False, **kwargs):
        """ Performs image classification on frames and returns
        a list of parsed objects with a confidence value.
        :param compute_cores:
        :param images_list: A list of image paths.
        :param model_name: Name of the face embeddings model to use.
        :param synchronous: (Optional) Default True for batching.
        """
        face_embeddings_model = FaceEmbeddingsExtractionModel(model_name)
        return self.run_inference(images_list, synchronous, compute_cores, face_embeddings_model)
