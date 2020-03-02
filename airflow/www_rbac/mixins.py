import configparser
import git
from pathlib import Path

from airflow.settings import GIT_CONF_PATH


class GitIntegrationMixin:
    MASTER_BRANCH = 'master'
    ORIGIN = 'origin'
    fs_path = None
    config_section = None
    _repo = None
    buttongroup_template = 'gitintegration/buttongroup.html'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)  # forwards all unused arguments
        self._repo = git.Repo.init(self.fs_path)
        # creating git conf file if it doesn't exists.
        # print('hehheheheeh')
        Path(GIT_CONF_PATH).touch(exist_ok=True)

    def git_pull(self, branch=MASTER_BRANCH):
        self._repo.remotes[branch].pull()

    def git_push(self, branch, origin=ORIGIN):
        self._repo.push(origin, branch)

    def git_checkout(self, branch):
        self._repo.checkout(branch)

    def git_commit(self, commit_msg, author):
        # TODO: Add a default author here.
        self._repo.commit('-m', commit_msg, author)

    def git_status(self):
        return self._repo.git.status('--porcelain')

    def git_commit_and_push(self, commit_msg, branch=MASTER_BRANCH):
        self.git_checkout(branch)
        self.git_commit(commit_msg)
        self.git_push(branch)

    def git_set_origin(self, origin_url):
        self._repo.remote('set-url', self.ORIGIN, origin_url)

    def read_config(self):
        conf = configparser.ConfigParser()
        conf.read(GIT_CONF_PATH)
        return conf

    def write_config(self, config):
        with open(GIT_CONF_PATH, 'w') as f:
            config.write(f)

    def get_section(self, section=None, config=None):
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
