import logging
import pickle

import numpy as np
from annoy import AnnoyIndex


class MatchingUtil:

    def __init__(self):
        return

    @staticmethod
    def load_annoy_index(index_file, vector_length=512):
        logging.info('Initialising matching utility...')
        index = AnnoyIndex(vector_length)
        index.load(index_file, prefault=True)
        logging.info('Annoy index {} is loaded'.format(index_file))
        with open(index_file + '.mapping', 'rb') as handle:
            mapping = pickle.load(handle)
        logging.info('Mapping file {} is loaded'.format(index_file + '.mapping'))
        logging.info('Matching utility initialised.')

        return index, mapping

    def find_similar_vectors(self, index, mapping, vector, num_matches):
        items = self.find_similar_items(index, mapping, vector, num_matches)
        vectors = [np.array(index.get_item_vector(item))
                   for item in items]
        return vectors

    @staticmethod
    def find_similar_items(index, mapping, vector, num_matches):
        similar_items, distances = index.get_nns_by_vector(
            vector, num_matches, search_k=-1, include_distances=True)
        # print(item_ids)
        identifiers = [mapping[item_id].decode("utf-8") for item_id in similar_items]
        return identifiers, distances
