#!/usr/bin/env python
# coding: utf8

from airflow.operators.tfserving.base_classes.base_helper import BaseHelper
from airflow.operators.tfserving.ioutils import conf as params
from airflow.operators.tfserving.interaction.collaborative_filtering.scripts import run_recommendation 
from airflow.operators.tfserving.ioutils.tf_serving_module import map_tfserving_model

class CollaborativeFilteringHelper(BaseHelper):

    def __init__(self):
        """ Default constructor. """
        super(BaseHelper, self).__init__()
        return

    def get_tags(self, input_path_dir, model_name,
                 compute_cores=params.three_tier_compute_cores,
                 compute_strategy=params.strategy_parallel,
                 input_extensions=params.media_type['rgb_stream']):


        mapped_fn = map_tfserving_model(params.TFServingModels.COLLAB_FILTERING_RECOMMENDATION.value)
        synchronous = False
        return self.run_helper(input_path_dir, input_extensions,
                               mapped_fn, synchronous,
                               compute_cores=compute_cores,
                               compute_strategy=compute_strategy,
                               model_name = model_name
                               )
