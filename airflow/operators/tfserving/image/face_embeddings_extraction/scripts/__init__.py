from airflow.operators.tfserving.image.face_embeddings_extraction.scripts.face_embeddings_extraction import \
    FaceEmbeddingsExtraction

run_face_embeddings_extraction = FaceEmbeddingsExtraction().run_face_embeddings_extraction
