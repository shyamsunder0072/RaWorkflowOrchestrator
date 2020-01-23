from airflow.lineage.datasets import File
from airflow.models import BaseOperator
from airflow.operators.papermill_operator import PapermillOperator, NoteBook
from airflow.utils.decorators import apply_defaults


class HTMLReport(File):
    type_name = 'html_report'


class CoutureJupyterOperator(PapermillOperator):

    @apply_defaults
    def __init__(self, input_nb, output_nb, parameters,
                 convert_to_html=False, output_html=None,
                 only_images=True, report_mode=False, *args, **kwargs):

        # TODO: Write convert to html code.
        if convert_to_html:
            assert output_html, 'Output html is not set, set it to a valid html path.'
            # self.outlets.append(HTMLReport(qualified_name=output_html,
            #                                location=output_html))

        if isinstance(input_nb, str):
            print('hereeee************************')
            super().__init__(input_nb=input_nb,
                             output_nb=output_nb,
                             parameters=parameters, *args, **kwargs)

        elif isinstance(input_nb, (tuple, list)):
            assert input_nb.__len__(), 'Empty input list/tuple of notebook files not allowed.'
            assert output_nb.__len__(), 'Empty output list/tuple of notebook files not allowed.'
            assert input_nb.__len__() == output_nb.__len__()

            inlets = [NoteBook(qualified_name=inp,
                               location=inp,
                               parameters=parameters) for inp in input_nb[:-1]]
            outlets = [NoteBook(qualified_name=out,
                                location=out) for out in output_nb[:-1]]
            super().__init__(input_nb=input_nb[:-1],
                             output_nb=output_nb[:-1],
                             parameters=parameters,
                             inlets=inlets, outlets=outlets,
                             *args, **kwargs)
        else:
            raise TypeError('expected str, list or tuple, found {}'.format(type(input_nb)))

    def execute(self, context):
        super().execute(context)
