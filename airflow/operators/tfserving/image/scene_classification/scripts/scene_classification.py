import os
import json
from airflow.operators.tfserving.base_classes.base_module import BaseModule
from airflow.operators.tfserving.image.scene_classification.models.scene_classification_model.scene_classification_model import SceneClassificationModel


class SceneClassification(BaseModule):

    def __init__(self):
        super().__init__()

    def run_inference_per_input(self, image_path, model, top_k, threshold):

        out_dictionary = model.run(image_path)

        out_dictionary = json.loads(out_dictionary.decode("utf-8"))

        final_dict = {}
        final_dict['class_preds'] = []
        final_dict['location'] = []
        final_dict['attributes'] = []
        final_dict['features'] = out_dictionary['features']

        for name, conf in out_dictionary['class_preds']:
            if(float(conf) > threshold):
                final_dict['class_preds'].append([name,conf])

        final_dict['location'].append(out_dictionary['location'])

        for name, conf in out_dictionary['attributes']:
            if(float(conf) > threshold):
                final_dict['attributes'].append([name,conf])

        final_dict['class_preds'].sort(key = lambda x: x[1], reverse = True)
        final_dict['attributes'].sort(key = lambda x: x[1], reverse = True)

        final_dict['class_preds'] = final_dict['class_preds'][:top_k]
        final_dict['attributes'] = final_dict['attributes'][:top_k]

        return {image_path: final_dict}

    def run_classification(self, image_path_list, model_name, top_k, threshold, compute_cores=1, synchronous=False, **kwargs):
        model = SceneClassificationModel(model_name)
        return self.run_inference(image_path_list, synchronous, compute_cores, model, top_k, threshold)
