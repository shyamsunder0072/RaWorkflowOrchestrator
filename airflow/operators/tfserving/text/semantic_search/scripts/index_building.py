import logging
import os
import pickle

import numpy as np
import tensorflow as tf
from annoy import AnnoyIndex


class IndexUtil(object):
    """Indexes records with constant embedding size"""
    def __init__(self):
        return

    @staticmethod
    def build_index(embedding_files, index_filename, metric="angular", num_trees=100):
        # annoy_index = AnnoyIndex(vector_length, metric=metric)
        annoy_index = None
        mapping = {}

        # change for tf2 upgrade : tf.gfile.Glob -> tf.io.gfile.glob
        embed_files = tf.io.gfile.glob(embedding_files)
        logging.info('{} embedding files are found.'.format(len(embed_files)))

        item_counter = 0
        for f, embed_file in enumerate(embed_files):
            logging.info('Loading embeddings in file {} of {}...'.format(
                f, len(embed_files)))
            # change for tf2 upgrade : tf.io.tf_record_iterator -> tf.compat.v1.python_io.tf_record_iterator
            record_iterator = tf.compat.v1.python_io.tf_record_iterator(path=embed_file)

            for string_record in record_iterator:
                example = tf.train.Example()
                example.ParseFromString(string_record)
                string_identifier = example.features.feature['item'].bytes_list.value[0]
                mapping[item_counter] = string_identifier
                embedding = np.array(
                    example.features.feature['embedding'].float_list.value)
                if not annoy_index:
                    annoy_index = AnnoyIndex(embedding.shape[0], metric=metric)
                annoy_index.add_item(item_counter, embedding)
                item_counter += 1

            logging.info('Loaded {} items to the index'.format(item_counter))

        logging.info('Start building the index with {} trees...'.format(num_trees))
        annoy_index.build(n_trees=num_trees)
        logging.info('Index is successfully built.')
        logging.info('Saving index to disk...')
        annoy_index.save(index_filename)
        logging.info('Index is saved to disk.')
        logging.info("Index file size: {} GB".format(
            round(os.path.getsize(index_filename) / float(1024 ** 3), 2)))
        print("Index file size: {} GB".format(
            round(os.path.getsize(index_filename) / float(1024 ** 3), 2)))
        annoy_index.unload()
        logging.info('Saving mapping to disk...')
        with open(index_filename + '.mapping', 'wb') as handle:
            pickle.dump(mapping, handle, protocol=pickle.HIGHEST_PROTOCOL)
        logging.info('Mapping is saved to disk.')
        logging.info("Mapping file size: {} MB".format(
            round(os.path.getsize(index_filename + '.mapping') / float(1024 ** 2), 2)))
        print("Mapping file size: {} MB".format(
            round(os.path.getsize(index_filename + '.mapping') / float(1024 ** 2), 2)))
