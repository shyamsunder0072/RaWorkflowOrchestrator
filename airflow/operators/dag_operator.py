from airflow.models import BaseOperator
from airflow.utils import timezone
from airflow.utils.decorators import apply_defaults
from airflow.api.common.experimental.run_dag import run_dag
from airflow.jobs.backfill_job import BackfillJob
import datetime
import six
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
    template_fields = ('run_dag_id', 'skip_dag', 'dag_run_conf')
    ui_color = '#e5c7f2'

    @apply_defaults
    def __init__(
            self,
            python_callable=None,
            skip_dag=False,
            dag_run_conf=None,
            *args, **kwargs):
        super(SkippableDagOperator, self).__init__(retries=0, *args, **kwargs)
        self.python_callable = python_callable
        self.skip_dag = skip_dag
        self.dag_run_conf = dag_run_conf

    def execute(self, context):
        execution_date = context['execution_date']

        if isinstance(execution_date, datetime.datetime):
            exe_date = execution_date.isoformat()
        elif isinstance(execution_date, six.string_types):
            exe_date = execution_date
        elif execution_date is None:
            exe_date = execution_date
        else:
            raise TypeError(
                'Expected str or datetime.datetime type '
                'for execution_date. Got {}'.format(
                    type(execution_date)))

        execution_date = timezone.parse(exe_date)

        if self.skip_dag:
            self.log.info("Skipped this, moving on")
            # self.__setstate__(State.SUCCESS)
        else:
            run_id = BackfillJob.ID_PREFIX_RUNDAG + exe_date

            dro = DagRunOrder(run_id=run_id)
            if self.python_callable is not None:
                dro = self.python_callable(context, dro)

            run_conf = dict(dro.payload or {})
            run_conf.update(context['dag_run'].conf or {})
            if isinstance(self.dag_run_conf, dict):
                run_conf.update(self.dag_run_conf or {})
            if dro:
                t = run_dag(dag_id=self.run_dag_id,
                            run_id=dro.run_id,
                            conf=run_conf,
                            replace_microseconds=False,
                            execution_date=execution_date)
            else:
                self.log.info("Criteria not met, moving on")

            t.refresh_from_db

            self.log.info(
                'Is wait over?? ... %s',
                t.get_state
            )


class DagOperator(SkippableDagOperator):

    ui_color = '#d575ff'
    @apply_defaults
    def __init__(
            self,
            *args, **kwargs):
        super(DagOperator, self).__init__(*args, **kwargs)
