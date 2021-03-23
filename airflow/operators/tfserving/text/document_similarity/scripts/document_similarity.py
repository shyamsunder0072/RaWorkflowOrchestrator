import re

import annoy
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import scipy
import seaborn as sns
from nltk.corpus import stopwords


class DocumentSimilarity(object):
    # index_filename = ""

    def __init__(self, model, similar_recommendation_count, ann_trees, ann_neighbors):
        self.model = model
        self.top_N = similar_recommendation_count
        self.ann_trees = ann_trees
        self.ann_neighbors = ann_neighbors

    def pre_processing_sentences(self, sentence):
        tags = re.sub("[^a-zA-Z]", " ", sentence).lower().split()
        stop_words = set(stopwords.words("english"))
        cleaned_tags = list(set([w for w in tags if w not in stop_words and w in self.model.wv.vocab]))
        return cleaned_tags

    def cosine_similarity_between_two_words(self, word1, word2):
        return 1 - scipy.spatial.distance.cosine(self.model[word1], self.model[word2])

    def calculate_heat_matrix_for_two_sentences(self, s1, s2):
        processed_s1 = self.pre_processing_sentences(s1)
        processed_s2 = self.pre_processing_sentences(s2)
        result_list = [[self.cosine_similarity_between_two_words(word1, word2)
                        for word2 in processed_s2] for word1 in processed_s1]
        heat_df = pd.DataFrame(result_list)
        heat_df.columns = processed_s2
        heat_df.index = processed_s1
        return heat_df

    def plot_heat_matrix_between_two_sentences(self, s1, s2):
        heat_df = self.calculate_heat_matrix_for_two_sentences(s1, s2)
        fig, ax = plt.subplots(figsize=(5, 5))
        ax_blue = sns.heatmap(heat_df, cmap="YlGnBu")
        return ax_blue

    def find_most_similar_matches(self, data, description_column):
        # data["tags"] = data[description_column].apply(lambda x: x.split())
        data["processed_tags"] = data[description_column].apply(lambda x: self.pre_processing_sentences(x))

        data = data[data["processed_tags"].map(lambda d: len(d)) > 0]

        data["mean_embedding"] = data["processed_tags"].apply(
            lambda x: np.mean([self.model[word] / (np.linalg.norm(self.model[word]) + 1e-16)
                               for word in x], axis=0))

        data = data[~np.any(list(map(np.isnan, data["mean_embedding"])), axis=1)]

        data["mean_embedding_normalised"] = data["mean_embedding"].apply(lambda x: x / (np.linalg.norm(x) + 1e-16))

        data = data.reset_index()

        embedding_dimensions = len(self.model["dog"])

        t = annoy.AnnoyIndex(embedding_dimensions, 'dot')
        for i in range(len(data)):
            v = data["mean_embedding_normalised"][i]
            t.add_item(i, v)

        t.build(self.ann_trees)
        # t.save(index_filename)

        most_similar = {}

        for i in range(len(data)):
            distance = {}
            neighbors = t.get_nns_by_item(i, self.ann_neighbors + 1)
            neighbors.remove(i)
            for n in neighbors:
                distance[n] = self.model.wmdistance(data["processed_tags"][i], data["processed_tags"][n])
            distance = sorted(distance.items(), key=lambda x: x[1])
            matches = distance[0: (self.top_N - 1)]
            closest_matches = [(data[description_column][i[0]], round(i[1], 2)) for i in matches]
            most_similar[data[description_column][i]] = closest_matches

        return most_similar
