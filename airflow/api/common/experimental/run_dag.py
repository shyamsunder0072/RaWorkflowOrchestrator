import json

from airflow.exceptions import DagRunAlreadyExists, DagNotFound
from airflow.models import DagRun, DagBag
from airflow.utils import timezone
from airflow.utils.state import State


def _run_dag(
    dag_id,
    dag_bag,
    dag_run,
    run_id,
    conf,
    replace_microseconds,
):
    if dag_id not in dag_bag.dags:
        raise DagNotFound("Dag id {} not found".format(dag_id))

    dag = dag_bag.get_dag(dag_id)
    execution_date = timezone.utcnow()
    assert timezone.is_localized(execution_date)

    if replace_microseconds:
        execution_date = execution_date.replace(microsecond=0)

    if not run_id:
        run_id = "manual__{0}".format(execution_date.isoformat())

    dr = dag_run.find(dag_id=dag_id, run_id=run_id)
    if dr:
        raise DagRunAlreadyExists("Run id {} already exists for dag id {}".format(
            run_id,
            dag_id
        ))

    run_conf = None
    if conf:
        if type(conf) is dict:
            run_conf = conf
        else:
            run_conf = json.loads(conf)

    runs = list()
    dags_to_trigger = list()
    dags_to_trigger.append(dag)

    from airflow.jobs import BackfillJob
    from airflow.executors import GetDefaultExecutor

    executor = GetDefaultExecutor()

    while dags_to_trigger:
        dag = dags_to_trigger.pop()
        trigger = dag.create_dagrun(
            run_id=run_id,
            execution_date=execution_date,
            state=State.RUNNING,
            conf=run_conf,
            external_trigger=True,
        )
        runs.append(trigger)
        if dag.subdags:
            dags_to_trigger.extend(dag.subdags)

        job = BackfillJob(dag=dag,
                          start_date=execution_date,
                          end_date=execution_date,
                          executor=executor,
                          donot_pickle=True)
        job.run()

    return runs


def run_dag(
    dag_id,
    run_id=None,
    conf=None,
    replace_microseconds=True,
):
    dagbag = DagBag()
    dag_run = DagRun()
    runs = _run_dag(
        dag_id=dag_id,
        dag_run=dag_run,
        dag_bag=dagbag,
        run_id=run_id,
        conf=conf,
        replace_microseconds=replace_microseconds,
    )

    return runs[0] if runs else None
