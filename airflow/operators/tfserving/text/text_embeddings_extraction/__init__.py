from airflow.operators.tfserving.text.text_embeddings_extraction.scripts import text_embeddings_extraction_module

embed_util = text_embeddings_extraction_module.TextEmbeddingsExtractionModule()
run_text_embeddings_extraction = embed_util.run_text_embeddings_extraction
