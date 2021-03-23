#!/usr/bin/env python
# coding: utf8
from airflow.operators.tfserving.base_classes.base_helper import BaseHelper
from airflow.operators.tfserving.ioutils import conf as params
from airflow.operators.tfserving.ioutils.pytorch_serving_module import map_pytorchserving_model


class InstanceSegmentationHelper(BaseHelper):

    def __init__(self):
        """ Default constructor. """
        super(BaseHelper, self).__init__()
        return

    def get_tags(self, dir_list,
                 output_dir_list,
                 compute_cores=params.three_tier_compute_cores,
                 compute_strategy=params.strategy_parallel,
                 input_extensions=params.media_type['image']):
    
        mapped_fn = map_pytorchserving_model(params.PytorchServingModels.INSTANCE_SEGMENTATION.value)
        synchronous = True
        return self.run_helper(dir_list, input_extensions,
                               mapped_fn, synchronous,
                               compute_cores=compute_cores,
                               compute_strategy=compute_strategy,
                               output_dir=output_dir_list
                               )

