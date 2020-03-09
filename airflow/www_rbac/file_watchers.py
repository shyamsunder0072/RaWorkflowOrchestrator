import time
from pathlib import Path
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

from airflow.settings import GIT_CONF_PATH, JUPYTER_HOME
from airflow.www_rbac.mixins import GitIntegrationMixin
# from airflow.www_rbac.views import JupyterNotebookView
from airflow.utils.log.logging_mixin import LoggingMixin

log = LoggingMixin().log


class GitFileSystemEventHandler(GitIntegrationMixin, FileSystemEventHandler):

    git_home_path = Path(GIT_CONF_PATH)

    class MicroMock(object):
        def __init__(self, **kwargs):
            self.__dict__.update(kwargs)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def on_any_event(self, event):
        # TODO: Make this method concurrent

        # Do nothing when there are changes in .git folders.
        if event.src_path.find(".git") != -1:
            return
        try:
            self.repo.git.add('.')
            self.git_commit("Auto commit by Workflow", self.MicroMock(
                username='workflow',
                email='workflow@couture.ai'))
        except Exception as e:
            log.error(e)
            return
        for conf in self.git_home_path.iterdir():
            if conf.is_file():
                # Putting it in a try catch wrapper because many errors can
                # exists, git conflicts, config section not configured properly
                # etc. We don't want error due to one conf affect push to
                # other git repositories.
                try:
                    config = self.read_config(rel_conf_path=conf)
                    section = self.get_section(config=config)
                    self.git_push(section=section)
                    log.info('Pushed changes to {} '.format(section['origin']))
                except Exception as e:
                    log.error(e)
        # print(event)
        # if event.is_directory:
        #     return None

        # elif event.event_type == 'created':
        #     # Event is created, you can process it now
        #     print("Watchdog received created event - % s." % event.src_path)
        # elif event.event_type == 'modified':
        #     # Event is modified, you can process it now
        #     print("Watchdog received modified event - % s." % event.src_path)

        # git commit and push for any event
        # print(GitIntegrationMixin.sections)


class GitFileSystemWatcher:

    observers = None

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.watch_paths = [
            {'path': JUPYTER_HOME, 'config_section': 'JupyterNotebook'},
        ]
        self.observers = []

    def init_observers(self):
        for watch_path in self.watch_paths:
            event_handler = GitFileSystemEventHandler(
                config_section=watch_path['config_section'],
                fs_path=watch_path['path'])
            observer = Observer()
            observer.schedule(event_handler, watch_path['path'], recursive=True)
            self.observers.append(observer)
            observer.start()

    def stop_observers(self):
        for observer in self.observers:
            observer.stop()
            log.info('Stopped observer::')

    def join_observers(self):
        try:
            while True:
                time.sleep(1)
        except Exception:
            self.stop_observers()
        for observer in self.observers:
            observer.join()

    def run(self):
        self.init_observers()
        self.join_observers()
