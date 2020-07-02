import configparser
import git
import os
from urllib.parse import urlparse, urlunparse, quote
from pathlib import Path
from flask import g, flash
from airflow.settings import GIT_CONF_PATH
from airflow import settings
from airflow.utils.log.logging_mixin import LoggingMixin

from kubernetes import config as kube_config
from kubernetes.client.api import core_v1_api
from kubernetes.client.rest import ApiException
from kubernetes.stream import stream

log = LoggingMixin().log

K8_JHUB_GIT_FS_PATH = '/home/jovyan/work/git_workspace'

class K8GitRepo:
    class git:
        '''Simple Git Class to work with k8s'''
        def __init__(self, fs_path, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.fs_path = fs_path

        def exec_kube_cmd(self, cmd, container=None):
            # TODO: Change this method.
            cmd = f'mkdir -p {K8_JHUB_GIT_FS_PATH} && cd {K8_JHUB_GIT_FS_PATH} && git init && {cmd}'
            cmd = ['/bin/bash', '-c', cmd]
            container = f'jupyter-{g.user.username}'
            log.info(f'Trying to execute command {cmd} in container {container}')
            # kube_config.load_incluster_config()
            kube_config.load_kube_config()
            core_v1 = core_v1_api.CoreV1Api()

            try:
                resp = core_v1.read_namespaced_pod(
                    name=container,
                    namespace='default')
            except Exception as e:
                if isinstance(e, ApiException) and e.status != 404:
                    log.info("Unknown error: %s" % e)
                elif isinstance(e, ApiException):
                    flash('Please open jupyterhub and start atleast one  server before executing git commands')
                else:
                    flash('Unknown error occured while executing git command !')
                return ''
            try:
                resp = stream(core_v1.connect_get_namespaced_pod_exec,
                    container,
                    'default',
                    command=cmd,
                    stderr=True, stdin=False,
                    stdout=True, tty=False)
            except Exception as e:
                log.info(e)
            # print(cmd)
            return cmd

        def pull(self, *args):
            cmd =  f'git pull {" ".join(args)}'
            return self.exec_kube_cmd(cmd)

        def push(self, *args):
            cmd =  f'git push {" ".join(args)}'
            return self.exec_kube_cmd(cmd)

        def config(self, *args):
            cmd =  f'git config {" ".join(args)}'
            return self.exec_kube_cmd(cmd)

        def commit(self, *args):
            cmd =  f'git commit {" ".join(args)}'
            return self.exec_kube_cmd(cmd)

        def log(self, *args):
            cmd =  f'git log {" ".join(args)}'
            return self.exec_kube_cmd(cmd)

        def add(self, *args):
            cmd =  f'git push {" ".join(args)}'
            return self.exec_kube_cmd(cmd)

        def init(self, *args):
            cmd =  f'git init {" ".join(args)}'
            return self.exec_kube_cmd(cmd)

        def status(self, *args):
            cmd =  f'git status {" ".join(args)}'
            return self.exec_kube_cmd(cmd)

    def __init__(self, fs_path, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.git = K8GitRepo.git(fs_path)


class GitIntegrationMixin:
    # MASTER_BRANCH = 'master'
    # ORIGIN = 'origin'
    fs_path = None
    config_section = None
    _repo = None
    git_template = 'gitintegration/buttongroup.html'

    # GLOBAL SECTIONS SET
    sections = set()
    fs_paths = set()

    keys = {
        'Origin': 'url',
        'Username': 'text',
        'Password': 'password',
        'Branch': 'text'
    }

    status_meanings = {
        'A': 'Added ',
        'D': 'Deleted ',
        'U': 'Unmerged ',
        'R': 'Renamed ',
        'C': 'Copied ',
        'M': 'Modified ',
        '?': 'Untracked '
    }

    def __init__(self, config_section=None, fs_path=None, *args, **kwargs):
        super().__init__(*args, **kwargs)  # forwards all unused arguments
        Path(GIT_CONF_PATH).mkdir(parents=True, exist_ok=True)

        if config_section:
            self.config_section = config_section
        if fs_path:
            self.fs_path = fs_path

        if self.config_section:
            self.sections.add(self.config_section)
        if self.fs_path:
            self.fs_paths.add(self.fs_path)

    @property
    def repo(self):
        if not self._repo:
            if self.fs_path:
                try:
                    if settings.RUNTIME_ENV == 'DOCKER':
                        # creating git repo if it doesn't exists.
                        self._repo = git.Repo.init(self.fs_path)
                    else:
                        # TODO: Call k8s here.
                        self._repo = K8GitRepo(K8_JHUB_GIT_FS_PATH)
                        # self._repo.git.init()
                except Exception as e:
                    log.error(e)
        return self._repo

    @classmethod
    def register_section(cls, section):
        cls.sections.append(section)

    def get_status(self):
        current_status = self.git_status()
        logs = self.git_logs("--pretty=%C(auto)%h %s, Author=<%aN>, Date=%ai")
        return current_status, logs

    @classmethod
    def get_sections(self):
        return self.sections

    def get_keys(self):
        return self.keys

    def get_git_template(self):
        return self.git_template

    def inject_username_and_password(self, url, username, password):
        username = quote(username)
        password = quote(password)

        parsed_url = urlparse(url)
        url = urlunparse(parsed_url._replace(
            netloc="{}:{}@{}".format(username, password, parsed_url.netloc)))
        return url

    def git_pull(self, branch=None, origin=None):
        section = self.get_section()
        origin = section['origin']
        username = section['username']
        password = section['password']
        origin = self.inject_username_and_password(origin, username, password)
        branch = section['branch']
        try:
            self.repo.git.pull(origin, branch)
        except git.exc.GitCommandError as err:
            log.error(err)
            return False
        return True

    def git_push(self, branch=None, origin=None, section=None):
        if not section:
            section = self.get_section()
        origin = section['origin']
        username = section['username']
        password = section['password']
        origin = self.inject_username_and_password(origin, username, password)
        branch = section['branch']
        try:
            self.repo.git.push(origin, branch)
        except git.exc.GitCommandError as err:
            log.error(err)
            return False
        return True

    def git_checkout(self, branch):
        self.repo.checkout(branch)

    def git_commit(self, commit_msg, author):
        # TODO: Add a default author here.
        self.repo.git.config('user.email', author.email)
        self.repo.git.config('user.name', author.username)
        self.repo.git.commit('-m',
                             commit_msg,
                             '--author', '{} <{}>'.format(author.username,
                                                          author.email))

    def __convert_logs(self, s):
        # s looks something like this: `13de430 Update abcd.txt`
        s = s.split()
        # s = (s[0:2], s[2:].strip())
        return {
            'hash': s[0],
            'msg': " ".join(s[1:]),
        }

    def git_logs(self, *args):
        try:
            logs = list(map(lambda s: self.__convert_logs(
                s), self.repo.git.log(*args).split('\n')))
        except git.exc.GitCommandError:
            logs = []
        return logs

        # return self._repo.log(*args)

    def git_add(self, files):
        if not isinstance(files, (list, tuple)):
            files = [files]
        for file in files:
            if file:
                file = file.strip('"')
                file.translate(str.maketrans({"-": r"\-",
                                              "]": r"\]",
                                              "\\": r"\\",
                                              "^": r"\^",
                                              "$": r"\$",
                                              "*": r"\*",
                                              ".": r"\."}))
                self.repo.git.add(file)

    def __convert_status(self, s):
        # s looks something like this: `?? filename`
        s = (s[0:2], s[2:].strip())
        status = ""
        for letter in s[0]:
            status += self.status_meanings.get(letter, letter)
        return {
            'status': status,
            'name': s[1]
        }

    def git_status(self):
        status = self.repo.git.status('--porcelain').split('\n')
        return list(filter(lambda s: True if s['name'] else False,
                           (map(lambda s: self.__convert_status(s), status))))

    def git_set_origin(self, origin_url):
        self.repo.remote('set-url', self.ORIGIN, origin_url)

    def read_config(self, rel_conf_path=None):
        if not rel_conf_path:
            rel_conf_path = g.user.username
        conf = configparser.ConfigParser()
        file_path = os.path.join(GIT_CONF_PATH, rel_conf_path)
        if not os.path.exists(file_path):
            Path(file_path).touch(exist_ok=True)
        conf.read(file_path)
        return conf

    def write_config(self, config, rel_conf_path=None):
        if not rel_conf_path:
            rel_conf_path = g.user.username
        # The git configs are user specific.
        with open(os.path.join(GIT_CONF_PATH, rel_conf_path), 'w') as f:
            config.write(f)

    def get_section(self, section=None, config=None):
        # print(config.sections())
        if not config:
            config = self.read_config()
        if not section:
            section = self.config_section
        return config[section]

    def write_section(self, section=None, config=None, **kwargs):
        if not config:
            config = self.read_config()
        if not section:
            section = self.config_section
        for key, val in kwargs.items():
            config[section][key] = val
        self.write_config(config)
