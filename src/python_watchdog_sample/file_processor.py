# __author = _amal
# python = 3.6

from python_watchdog_sample.util import Watcher


class FileProcessor:

    def __init__(self, sftp_directory):
        self._directory_to_watch = sftp_directory

    def run_copy(self):
        w = Watcher(self._directory_to_watch)
        w.run()


if __name__ == '__main__':
    f = FileProcessor('/Users/ambabu/Documents/PersonalDocuments/code-samples/watch-dog-example/resources/')
    f.run_copy()





