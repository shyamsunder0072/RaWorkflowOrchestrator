import gc
import os
from collections import ChainMap

from airflow.operators.tfserving.ioutils import conf as params
from airflow.operators.tfserving.ioutils.multiprocessing import MyPool


class BaseHelper(object):
    """ base class to be extended by every helper for standard code conventions. """

    def __init__(self):
        self.input_list = None

    def run_helper(self, dir_list, supported_file_extensions, inference_function, synchronous,
                   compute_cores: int = params.three_tier_compute_cores,
                   compute_strategy: params.ComputeStrategy = params.default_inner_compute_strategy,
                   **kwargs):
        input_list = self.collect_inputs(dir_list, supported_file_extensions, inference_function, compute_cores,
                                         synchronous, **kwargs)

        return self.run_inferences(input_list, compute_cores, compute_strategy)

    @staticmethod
    def collect_inputs(dir_list, supported_file_extensions, inference_function, compute_cores, synchronous=True, **kwargs):
        input_list = []
        total_dirs = len(dir_list)
        for i in range(total_dirs):
            dir_ = dir_list[i]
            sub_input_list = []
            for ext_type in supported_file_extensions:
                # sub_input_list.extend(glob(os.path.join(dir_, ext_type)))
                sub_input_list.extend([os.path.join(dir_, x) for x in os.listdir(dir_) if x.endswith(ext_type)])
            # do not send empty input lists for inference
            if sub_input_list:
                task_args = {'compute_cores': compute_cores, 'synchronous': synchronous}
                for k, v in kwargs.items():
                    if type(v) == list:
                        if len(v) == 1:
                            task_args[k] = v[0]
                        else:
                            task_args[k] = v[i]
                    else:
                        task_args[k] = v

                input_list.append((inference_function, sub_input_list, task_args))
        # self.input_list = input_list
        return input_list

    @staticmethod
    def mapped_inference_function(inference_function, input_list, task_args):
        return inference_function(input_list, **task_args)

    def run_inferences(self, input_list, compute_cores, compute_strategy):
        result_list = []
        dir_wise_output = dict()
        self.input_list = input_list
        if self.input_list:
            if compute_strategy == params.strategy_parallel:
                with MyPool(processes=min(len(self.input_list), compute_cores), maxtasksperchild=params.tasks_per_child) \
                        as inner_task_pool:
                    result_list = inner_task_pool.starmap(self.mapped_inference_function, self.input_list)
                gc.collect()
            elif compute_strategy == params.strategy_sequential:
                for sub_input_list in self.input_list:
                    result_list.append(self.mapped_inference_function(*sub_input_list))

            # aggregate output
            if result_list and [i for i in result_list if i]:
                for result in result_list:
                    input_dir = os.path.dirname(list(result[0].keys())[0])
                    dir_wise_output[input_dir] = {os.path.basename(k): v for k, v in dict(ChainMap(*result)).items()}

        return dir_wise_output
