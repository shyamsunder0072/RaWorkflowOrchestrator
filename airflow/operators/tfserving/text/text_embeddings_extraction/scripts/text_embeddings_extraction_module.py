import pandas as pd

from airflow.operators.tfserving.base_classes.base_module import BaseModule
from airflow.operators.tfserving.text.text_embeddings_extraction.models.version1 import conf as model_config
from airflow.operators.tfserving.text.text_embeddings_extraction.models.version1.text_embeddings_extraction_model import \
    TextEmbeddingsExtractionModel


class TextEmbeddingsExtractionModule(BaseModule):

    text_embeddings_extraction_model = TextEmbeddingsExtractionModel()

    def __init__(self):
        self._batch_size = model_config.batch_size
        super().__init__()

    def run_inference_per_input(self, sentences):
        return self.text_embeddings_extraction_model.run(sentences)

    def run_text_embeddings_extraction(self, embedding_file_list, item_col='claim_id', text_col='tags_document',
                                       category_col='category', categories_list=None, compute_cores=1, synchronous=False,
                                       **kwargs):
        embed_result_list = []
        for embedding_file in embedding_file_list:
            if embedding_file.endswith('parquet'):
                df = pd.read_parquet(embedding_file)
            elif embedding_file.endswith('csv'):
                df = pd.read_csv(embedding_file)
            else:
                print("Skipping:{}\nEmbedding file should either parquet or csv".format(embedding_file))
                continue
            if categories_list is not None:
                df = df[df[category_col].isin(categories_list)]
            items = df[item_col].values
            queries = df[text_col].values
            batch_argument_list = []
            for i in range(0, len(items), self._batch_size):
                batch_argument_list.append(queries[i:i+self._batch_size])
            embeddings = self.run_inference(batch_argument_list, synchronous, compute_cores)
            embeddings = [i for batch_embeddings in embeddings for i in batch_embeddings]
            result_list = [{item: embedding} for item, embedding in zip(items, embeddings)]
            embed_result_list.append({embedding_file: result_list})
        return embed_result_list
