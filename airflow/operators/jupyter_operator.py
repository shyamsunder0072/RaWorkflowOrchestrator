import os
import papermill as pm
from nbconvert import HTMLExporter
from airflow.lineage.datasets import File
from airflow.operators.papermill_operator import PapermillOperator, NoteBook
from airflow.settings import JUPYTER_HOME
from airflow.utils.decorators import apply_defaults
from pathlib import Path

from traitlets.config import Config


class HTMLReport(File):
    type_name = 'html_report'


class CoutureJupyterOperator(PapermillOperator):
    ui_color = '#F2D7D5'

    """
    Runs a `.ipynb` notebook and produces an output `.ipynb` file with the
    output results. Can also produce an html alongside.

    It can also take parameters to be injected into the notebook to override
    variables in the notebook cell tagged `parameters`.

    NOTE: All paths are relative to jupyter notebook home directory.
    :param input_notebook: path to input .ipynb file.
    :type input_notebook: str
    :param output_notebook: path to output .ipynb file. This is optional parameter, if not provided, input notebook will be overwritten with outputs
    :type output_notebook: str
    :param parameters: parameters to be overriden.
    :type parameters: dict
    :param export_html: Whether to export the result of the output notebook into html.
        Defaults to False.
    :type export_html: boolean
    :param output_html: path to a valid directory, where the html file will be saved.
    :type output_html: str
    :param report_mode: Whether to hide ingested parameters. Defaults to True.
    :type report_mode: boolean
    """
    __html_config = Config()
    __html_config.HTMLExporter.preprocessors = [
        'nbconvert.preprocessors.ExtractOutputPreprocessor']

    __html_exporter = HTMLExporter(config=__html_config)

    @apply_defaults
    def __init__(self, input_notebook, output_notebook, parameters,
                 export_html=False, output_html=None,
                 report_mode=True, *args, **kwargs):

        input_notebook = os.path.join(JUPYTER_HOME, input_notebook)

        if output_notebook:
            output_notebook = os.path.join(JUPYTER_HOME, output_notebook)
        else:
            output_notebook = input_notebook

        super().__init__(input_nb=input_notebook,
                         output_nb=output_notebook,
                         parameters=parameters, *args, **kwargs)
        self.report_mode = report_mode

        # TODO: Write convert to html code.
        if export_html:
            if not output_html:
                nb_name = Path(input_notebook).resolve().stem
                output_html = nb_name + ".html"

            assert output_html, 'output_html is not set, set it to a valid folder path.'
            output_html = os.path.join(JUPYTER_HOME, output_html)
            # self.outlets.append(HTMLReport(qualified_name=output_html,
            #                                location=output_html))
            self.export_html = export_html
            self.output_html = output_html
            os.makedirs(self.output_html, exist_ok=True)
            self.inlets.append(NoteBook(qualified_name=output_notebook,
                                        location=output_notebook))
            self.outlets.append(HTMLReport(name=output_html))

        # NOTE: The below code handles multiple notebooks at a time.
        # if isinstance(input_nb, str):
        # elif isinstance(input_notebook, (tuple, list)):
        #     assert len(
        #         input_notebook), 'Empty input list/tuple of notebook files not allowed.'
        #     assert len(
        #         output_notebook), 'Empty output list/tuple of notebook files not allowed.'
        #     # No Args passed
        #     if not len(parameters):
        #         parameters = [{} for i in input_notebook]
        #     assert len(input_notebook) == len(output_notebook) and len(
        #         input_notebook) == len(parameters)

        #     inlets = [NoteBook(qualified_name=input_notebook[i],
        #                        location=input_notebook[i],
        #                        parameters=parameters[i]) for i in range(len(input_notebook))]
        #     outlets = [NoteBook(qualified_name=out,
        #                         location=out) for out in output_notebook]
        #     # print(inlets, outlets, parameters[-1])

        #     # NOTE: Papermill operator is buggy. Refactor this when any changes are made there.
        #     super().__init__(input_notebook=input_notebook[0],
        #                      output_notebook=output_notebook[0], parameters=parameters[0], *args, **kwargs)

        #     self.inlets = []
        #     self.outlets = []
        #     for i in range(len(inlets)):
        #         self.inlets.append(inlets[i])
        #         self.outlets.append(outlets[i])
        #     # super().__init__(input_nb=input_notebook[-1],
        #     #                  output_nb=output_notebook[-1],
        #     #                  parameters=parameters[-1],
        #     #                  inlets={ "jupyter_notebook": inlets },
        #                        outlets={ "jupyter_notebook": outlets},
        #     #                  *args, **kwargs)
        # else:
        #     raise TypeError(
        #         'expected str, list or tuple, found {}'.format(type(input_notebook)))

    def execute(self, context):
        # super().execute(context)

        for i in range(len(self.inlets)):
            if isinstance(self.outlets[i], HTMLReport):
                (_, resources_with_fig) = self.__html_exporter.from_file(
                    self.inlets[i].location)
                for res in resources_with_fig['outputs'].keys():
                    f = open(os.path.join(self.output_html, res), 'wb')
                    f.write(resources_with_fig['outputs'][res])
                    f.close()

                with open(os.path.join(self.output_html, 'index.html'), 'w') as html_file:
                    html_file.write(_)

            else:
                # output is a jupyter notebook
                pm.execute_notebook(self.inlets[i].location, self.outlets[i].location,
                                    parameters=self.inlets[i].parameters,
                                    progress_bar=False, report_mode=self.report_mode)
