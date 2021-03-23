import gc
import multiprocessing


class BaseModule(object):
    def __init__(self):
        return

    def run_inference_per_input(self, *args, **kwargs):
        raise NotImplementedError

    def run_inference(self, input_list, synchronous, compute_cores, *args):
        for arg in args:
            if arg is None:
                raise TypeError("Some of the required arguments are not provided or set to None.")
        argument_list = [[input_data, *args] for input_data in input_list]
        result_list = []
        if synchronous:
            with multiprocessing.Pool(processes=min(compute_cores, len(argument_list)),
                                      maxtasksperchild=1000) as multi_inputs_pool:
                result_list = multi_inputs_pool.starmap(self.run_inference_per_input, argument_list)
            gc.collect()
        else:
            for argument in argument_list:
                result_list.append(self.run_inference_per_input(*argument))
        return result_list
