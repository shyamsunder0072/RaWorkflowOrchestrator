from airflow.operators.tfserving.base_classes.base_module import BaseModule
from airflow.operators.tfserving.interaction.collaborative_filtering.models.version1.collab_filtering_model import CollabFilteringModel
import os
import numpy as np

#change names to generic - can be else
class CollabFilteringRecommendationModule(BaseModule):

    def __init__(self):
        super().__init__()

    def run_inference_per_input(self, input_data_path, model):
        
        basename = os.path.splitext(os.path.basename(input_data_path))[0]
        input_data = np.load(input_data_path).astype(np.int32)
        predictions = model.run(input_data)

        return {input_data_path: {'users': input_data, 'predictions': predictions}}

    def run_recommendation(self, user_id_files, compute_cores=1, synchronous=False, model_name = None, **kwargs):
        model = CollabFilteringModel(model_name)
        return self.run_inference(user_id_files, synchronous, compute_cores, model)
