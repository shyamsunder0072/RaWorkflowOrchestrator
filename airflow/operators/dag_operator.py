from airflow.models import BaseOperator
from airflow.utils import timezone
from airflow.utils.decorators import apply_defaults
from airflow.api.common.experimental.run_dag import run_dag

import json


class DagRunOrder(object):
    def __init__(self, run_id=None, payload=None):
        self.run_id = run_id
        self.payload = payload


class SkippableDagOperator(BaseOperator):
    """
    Runs DAG of specified ``dag_id``

    :param run_dag_id: the dag_id to run (templated)
    :type run_dag_id: str
    :param python_callable: a reference to a python function that will be
        called while passing it the ``context`` object and a placeholder
        object ``obj`` for your callable to fill and return if you want
        a DagRun created. This ``obj`` object contains a ``run_id`` and
        ``payload`` attribute that you can modify in your function.
        The ``run_id`` should be a unique identifier for that DAG run, and
        the payload has to be a picklable object that will be made available
        to your tasks while executing that DAG run. Your function header
        should look like ``def foo(context, dag_run_obj):``
    :type python_callable: python callable
    """
    template_fields = ('run_dag_id', 'skip_dag')
    ui_color = '#dda0dd'

    @apply_defaults
    def __init__(
        self,
        python_callable=None,
        skip_dag=False,
        *args, **kwargs):
        super(SkippableDagOperator, self).__init__(*args, **kwargs)
        self.python_callable = python_callable
        self.skip_dag = skip_dag


    def execute(self, context):
        if self.skip_dag:
            self.log.info("Skipped this, moving on")
            #self.__setstate__(State.SUCCESS)
        else:
            run_id = 'run__' + timezone.utcnow().isoformat()
            dro = DagRunOrder(run_id=run_id)
            if self.python_callable is not None:
                dro = self.python_callable(context, dro)

            if dro:
                t = run_dag(dag_id=self.run_dag_id,
                            run_id=dro.run_id,
                            conf=json.dumps(dro.payload),
                            replace_microseconds=False)
            else:
                self.log.info("Criteria not met, moving on")

            t.refresh_from_db

            self.log.info(
                'Is wait over?? ... %s',
                t.get_state
            )



class DagOperator(SkippableDagOperator):

    ui_color = '#ab6cc6'
    @apply_defaults
    def __init__(
        self,
        *args, **kwargs):
        super(DagOperator, self).__init__(*args, **kwargs)
