# Workflow Orchestrator

[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/apache-airflow.svg)](https://pypi.org/project/apache-airflow/)

Workflow Orchestrator is a platform build on Apache Airflow.

## Getting started
Workflow Orchestrator is easy to run since it is present in a **Dockerized** format. Docker image can be pulled from here.

To install the orchestrator, download docker-compose file from here.

Run `docker-compose` command.

```bash
COUTURE_WORKFLOW_USER=<your name> docker-compose up worker
```
That's it, **visit localhost:8080 in the browser** to start using orchestrator.
RBAC is used to provide security, reach out to [Couture.ai](https://www.couture.ai) for more details.

After logging in as Admin:

1. New dags can be added/deleted through:
   Admin->Add DAG
   
1. Confirgurations for submitting spark jobs, can be configured through:
   Admin->Couture Spark Configuration

1. Confirgurations for hadoop, can be configured through:
   Admin->Couture Hadoop Configuration

1. Artifacts for spark jobs can be added/deleted from (Formats: *.jars, *.py, *.egg, *.zip):
   Admin->Upload Artifact

1. Edit existing dags through:
   Admin->Dag code editor

## Beyond the Horizon (Much more than Airflow)

1. Easily configure spark jobs through GUI
1. Add hadoop configurations through GUI
1. Design end-to-end pipeline by creating master dag, i.e. DAG of dags
1. Add/delete/edit dags through GUI
1. Track user journey by audit logging
1. Add task level description
1. Personalised dag view (by setting environment variable COUTURE_WORKFLOW_USER)

Committers:

* Refer to [Committers](https://www.couture.ai)

## Who Created Workflow Orchestrator?

Workflow Orchestrator is the work of the [couture.ai](https://www.couture.ai)
