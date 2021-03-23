from gensim.models.fasttext import FastText

from airflow.operators.tfserving.text.document_similarity.scripts import document_similarity

model = FastText.load_fasttext_format("couture_dl/pretrained_models/word2vec_embeddings/cc.en.300.bin")

_script_helper_obj = document_similarity.DocumentSimilarity(model=model, similar_recommendation_count=10,
                                                            ann_trees=20, ann_neighbors=100)
find_similar_documents = _script_helper_obj.find_most_similar_matches



