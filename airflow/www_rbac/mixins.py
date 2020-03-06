import configparser
import git
from urllib.parse import urlparse, urlunparse, quote
from pathlib import Path
import os
from flask import g
from airflow.settings import GIT_CONF_PATH
from airflow.utils.log.logging_mixin import LoggingMixin

log = LoggingMixin().log


class GitIntegrationMixin:
    # MASTER_BRANCH = 'master'
    # ORIGIN = 'origin'

    fs_path = None
    config_section = None
    _repo = None
    git_template = 'gitintegration/buttongroup.html'

    # GLOBAL SECTIONS SET
    sections = set()

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

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)  # forwards all unused arguments
        try:
            self._repo = git.Repo.init(self.fs_path)
            # creating git conf file if it doesn't exists.
        except Exception as e:
            log.error(e)

        Path(GIT_CONF_PATH).mkdir(exist_ok=True)

        if self.config_section:
            self.sections.add(self.config_section)

    @classmethod
    def register_section(cls, section):
        cls.sections.append(section)

    def get_status(self):
        current_status = self.git_status()
        logs = self.git_logs('--oneline')
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
            self._repo.git.pull(origin, branch)
        except git.exc.GitCommandError as err:
            log.error(err)
            return False
        return True

    def git_push(self, branch=None, origin=None):
        section = self.get_section()
        origin = section['origin']
        username = section['username']
        password = section['password']
        origin = self.inject_username_and_password(origin, username, password)
        branch = section['branch']
        try:
            self._repo.git.push(origin, branch)
        except git.exc.GitCommandError as err:
            log.error(err)
            return False
        return True

    def git_checkout(self, branch):
        self._repo.checkout(branch)

    def git_commit(self, commit_msg, author):
        # TODO: Add a default author here.
        self._repo.git.config('user.email', author.email)
        self._repo.git.config('user.name', author.username)
        self._repo.git.commit('-m',
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
            logs = list(map(lambda s: self.__convert_logs(s), self._repo.git.log(*args).split('\n')))
        except git.exc.GitCommandError:
            logs = []
        return logs

        # return self._repo.log(*args)

    def git_add(self, files):
        if not isinstance(files, (list, tuple)):
            files = [files]
        for file in files:
            # TODO: change author here.
            if file:
                self._repo.git.add(file)

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
        status = self._repo.git.status('--porcelain').split('\n')
        return list(filter(lambda s: True if s['name'] else False,
                           (map(lambda s: self.__convert_status(s), status))))

    def git_set_origin(self, origin_url):
        self._repo.remote('set-url', self.ORIGIN, origin_url)

    def read_config(self):
        conf = configparser.ConfigParser()
        file_path = os.path.join(GIT_CONF_PATH,  g.user.username)
        if not os.path.exists(file_path):
            Path(file_path).touch(exist_ok=True)
        conf.read(file_path)
        return conf

    def write_config(self, config):
        with open(os.path.join(GIT_CONF_PATH,  g.user.username), 'w') as f:
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
