# Setting up Couture Workflow Orchestrator

### Pre-requisites:
- [Apache Airflow](https://airflow.apache.org/)
- Python (3.6/3.7)
- Node (v10.0-12.0)
- NVM - Node version manager
- MySQL

### Steps

#### Clone Repository
Clone the repository from [here](https://github.com/coutureai/RaWorkflowOrchestrator)

#### Setup Python Environment
Run the following commands in your project root:
```bash
python3 -m venv env
source env/bin/activate
```

#### Install python packages
Run the following commands in your project root:
```bash
pip3 install Cython
pip3 install gitpython
pip3 install -r requirements/requirements-python<enter_python_version>.txt
pip3 install SQLAlchemy==1.3.16 
```

#### Run Setup
Run the following commmands from your project root:
```bash
python3 setup.py install
python3 setup.py build
```

#### Build Front End
Run the following from your project root:
```bash
cd airflow/www_rbac
nvm install 10.0
nvm use 10.0
yarn install
yarn build
```

#### Create Database and User:
Run the following from your project root:
```bash
airflow initdb
airflow create_user -u <username> -e <user_email> -f <first name> -l <last_name> -p <password> -r Admin
```

#### Changes to Airflow Configuration
Find the airflow configuration file, it's likely to be located at ```~/airflow/airflow.cfg```, and add the following to your ```airflow.cfg```, under ```AIRFLOW_HOME```:
```
[langserver]
langserver_path = /langserver/python
langserver_port = 8088
# this differs from langserver_path in the sense you can provide hostname and/or port here.
# example: langserver_url = ws://stackoverflow.com/langserver/python
langserver_url = ws://localhost:8088/langserver/python

[modelservers]
pytorch_model_server_host = pytorchserving
pytorch_model_server_management_port = 8060
pytorch_model_server_inference_port = 8601
```

Additionally, change the ```rbac``` setting to True.
```
rbac = True
```
#### Launch
Run the following in your project root:
```
airflow webserver -p <port_number>
```

The Orchestrator would launch in the selected port at localhost:port