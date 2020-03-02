import configparser
import git
from urllib.parse import urlparse, urlunparse, quote
from pathlib import Path

from airflow.settings import GIT_CONF_PATH


class GitIntegrationMixin:
    MASTER_BRANCH = 'master'
    ORIGIN = 'origin'
    fs_path = None
    config_section = None
    _repo = None
    git_template = 'gitintegration/buttongroup.html'

    status_meanings = {
        'A': 'Added ',
        'D': 'Deleted ',
        'U': 'Unmerged ',
        'R': 'Renamed ',
        'C': 'Copied ',
        'M': 'Modified ',
        '?': 'Untracked '
    }

    def get_git_template(self):
        return self.git_template

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)  # forwards all unused arguments
        self._repo = git.Repo.init(self.fs_path)
        # creating git conf file if it doesn't exists.
        # print('hehheheheeh')
        Path(GIT_CONF_PATH).touch(exist_ok=True)

    def inject_username_and_password(self, url, username, password):
        username = quote(username)
        password = quote(password)

        parsed_url = urlparse(url)
        url = urlunparse(parsed_url._replace(netloc="{}:{}@{}".format(username, password, parsed_url.netloc)))
        return url

    def git_pull(self, branch=None, origin=None):
        section = self.get_section()
        origin = section['origin']
        username = section['username']
        password = section['password']
        origin = self.inject_username_and_password(origin, username, password)
        branch = section['branch']
        self._repo.git.pull(origin, branch)

    def git_push(self, branch=None, origin=None):
        section = self.get_section()
        origin = section['origin']
        username = section['username']
        password = section['password']
        origin = self.inject_username_and_password(origin, username, password)
        branch = section['branch']
        self._repo.git.push(origin, branch)

    def git_checkout(self, branch):
        self._repo.checkout(branch)

    def git_commit(self, commit_msg, author):
        # TODO: Add a default author here.
        self._repo.git.commit('-m',
                              commit_msg,
                              '--author', '{} <{}>'.format(author.username,
                                                           author.email))

    def git_add(self, files):
        if not isinstance(files, (list, tuple)):
            files = [files]
        for file in files:
            # TODO: change author here.
            self._repo.git.add(file)

    def __convert_status(self, s):
        # s looks something like this: `?? filename`
        s = (s[0:2], s[2:].strip())
        status = ""
        # if s[0] == "??":
        #     status = "Untracked"
        # else:
        for letter in s[0]:
            status += self.status_meanings.get(letter, letter)
        # s[0] = self.status_meanings.get(s[0], s[0])
        return [status, s[1]]

    def git_status(self):
        # for i in self._repo.git.status('--porcelain'):
        #     print(i)
        return list(map(lambda s: self.__convert_status(s), self._repo.git.status('--porcelain').split('\n')))

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
