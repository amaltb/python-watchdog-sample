# __author = _amal
# python = 3.6

import os
import time
import traceback
import pandas as pd
from subprocess import Popen, PIPE

from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer
from python_watchdog_sample.constants import VALID_CSV_HEADER, HDFS_DESTINATION, CSV_FILE_DELIMITER
from python_watchdog_sample.config import logger


def copy_to_hdfs(source, destination):
    ret, output, error = run_cmd(["hadoop", "fs", "-put", source, destination])
    if ret != 0:
        return RuntimeError('Unable to copy to hdfs location....\n Error: ' + error)


def run_cmd(args_list):
    print('Running system command: {0}'.format(' '.join(args_list)))
    proc = Popen(args_list, stdout=PIPE, stderr=PIPE)
    s_output, s_err = proc.communicate()
    s_return = proc.returncode
    return s_return, s_output, s_err


def get_file_name(file_path: str):
    if file_path is None or not file_path.__contains__(os.sep):
        return ValueError("Not a valid file path...")
    else:
        path_list = file_path.split(os.sep)
        path_list.reverse()
        return path_list[0]


def validate_csv_file(file_path: str, header: str):
    try:
        df = pd.read_csv(file_path, sep=CSV_FILE_DELIMITER)
    except Exception:
        logger.exception('Not a valid CSV...')
        return False

    if header.split(',') != list(df.columns.values):
        logger.exception('Header does not match...')
        return False

    return True


class Watcher:
    def __init__(self, directory_to_watch):
        self.observer = Observer()
        self._directory_to_watch = directory_to_watch

    def run(self):
        event_handler = Handler()
        self.observer.schedule(event_handler, self._directory_to_watch, recursive=True)
        self.observer.start()
        try:
            while True:
                time.sleep(5)
        except InterruptedError:
            self.observer.stop()
            print("Error: " + traceback.format_exc())

        self.observer.join()


class Handler(FileSystemEventHandler):

    @staticmethod
    def on_any_event(event):
        if event.is_directory:
            return None

        elif event.event_type == 'created':
            # Take any action here when a file is first created.
            logger.info("Received created event - %s." % event.src_path)
            file_path = str(event.src_path)
            file_name = get_file_name(file_path)

            # Checking if the file created is a valid csv file...
            if validate_csv_file(file_path, VALID_CSV_HEADER):
                try:
                    copy_to_hdfs(file_path, HDFS_DESTINATION)
                except Exception:
                    logger.exception("Exception while copying to hdfs...")
            else:
                logger.error("File " + file_name + " is not a valid csv file..")

        elif event.event_type == 'modified':
            # Taken any action here when a file is modified.
            pass
