# __author = _amal
# python = 2.7
import json
import os
from subprocess import Popen, PIPE

import pandas as pd
import re
import sys
from os import listdir
from os.path import isfile, join, exists

import logging

# creating log file directory...
if not os.path.exists('./log'):
    os.makedirs('./log')

# creating tracker file directory...
if not os.path.exists('./track'):
    os.makedirs('./track')

# Create a custom logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
# Create handlers
c_handler = logging.StreamHandler()
# Generating log file in the directory where the script file is kept...
f_handler = logging.FileHandler('./log/application.log')
c_handler.setLevel(logging.DEBUG)
f_handler.setLevel(logging.DEBUG)

# Create formatters and add it to handlers
c_format = logging.Formatter('%(name)s - %(levelname)s - %(message)s')
f_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
c_handler.setFormatter(c_format)
f_handler.setFormatter(f_format)

# Add handlers to the logger
logger.addHandler(c_handler)
logger.addHandler(f_handler)

_SOURCE_DIR = None
_HDFS_DESTINATION_ROOT_DIR = None
_INVALID_FILES_HDFS_DIR = None
_TRACKER_FILE_REGEX = '^(csv_Tracker_)[0-9]+(\.csv)$'
_CSV_FILE_DELIMITER = ','
_TRACKER_FILE_HEADER = 'NO#,Frequency,week,Category,Type,claimcnt,File Name'

_FEED_FILE_TYPE_NAME_MAP = {
    'Paid-Paid': '^[A-Za-z0-9_]*(PAID_CVS_PAID)[A-Za-z0-9_]*(\.csv)$',
    'paid-Reject': '^[A-Za-z0-9_]*(PAID_CVS_REJECTED)[A-Za-z0-9_]*(\.csv)$',
    'Reject-Reject': '^[A-Za-z0-9_]*(REJECTED_CVS_REJECTED)[A-Za-z0-9_]*(\.csv)$',
    'Reject-Paid': '^[A-Za-z0-9_]*(REJECTED_CVS_PAID)[A-Za-z0-9_]*(\.csv)$',
}


def run():
    try:
        tracker_files = _get_tracker_files()
        if not tracker_files:
            logger.info('No tracker files to process')
            exit(0)

        for f in tracker_files:
            _process_tracker_file(f)
    except RuntimeError:
        logger.exception('Execution failed...')


def _process_tracker_file(tracker_file):
    logger.info('Processing started for tracker file: ' + tracker_file)
    valid_feed_files, invalid_feed_files = _get_feed_files(tracker_file)

    if _check_if_present(valid_feed_files):

        # building valid and invalid destination paths
        destination_dt = _build_destination_path_dt(tracker_file)
        valid_destination_path = join(_HDFS_DESTINATION_ROOT_DIR, destination_dt)
        invalid_destination_path = join(_INVALID_FILES_HDFS_DIR, destination_dt)

        files_to_delete = []

        for f in valid_feed_files:
            if _copy_to_hdfs(f, valid_destination_path):
                files_to_delete.append(f)

        for f in invalid_feed_files:
            if _copy_to_hdfs(f, invalid_destination_path):
                files_to_delete.append(f)

        track_data = {
            tracker_file: [f for f in files_to_delete if f not in invalid_feed_files and f != tracker_file]
        }

        # check if all the feed files and tracker file, are successfully copied to hdfs, before calling delete
        if files_to_delete.__len__() == (len(valid_feed_files) + len(invalid_feed_files)) \
                and _copy_to_hdfs(tracker_file, valid_destination_path):
            files_to_delete.append(tracker_file)
            _delete_files(files_to_delete)

        # writing to the tracker file
        tracker_file = join('./track', tracker_file + '_tracker.json')
        try:
            with open(tracker_file, 'w') as outfile:
                json.dump(track_data, outfile)
        except Exception as e:
            raise RuntimeError('Failed to write the tracker file due to exception...' + str(e))

    else:
        logger.info('Aborting execution as all the feed files are not present at the source. '
                    'Try again in next execution...')

    logger.info('Processing finished for tracker file: ' + tracker_file)


def _build_destination_path_dt(tracker_file_name):
    m = re.search('Tracker_(.+?)(\.csv)$', tracker_file_name)
    if m:
        dt = str(m.group(1))
        yr = dt[4:]
        day = dt[2:4]
        mo = dt[:2]
        return yr + os.sep + mo + os.sep + day
    else:
        raise RuntimeError('Could not build destination path using tracker file. Check tracker file name...')


def _delete_files(list_of_files):
    l1 = [join(_SOURCE_DIR, f) for f in list_of_files]

    for f in l1:
        if exists(f):
            os.remove(f)


def _copy_to_hdfs(filename, destination_dir):
    source = join(_SOURCE_DIR, filename)

    # check if destination directory exist or not
    ret, output, error = run_cmd(["hadoop", "fs", "-test", "-d", destination_dir])
    if ret != 0:
        # creating destination directory
        ret, output, error = run_cmd(["hadoop", "fs", "-mkdir", "-p", destination_dir])
        if ret != 0:
            raise RuntimeError('Unable to create destination hdfs directory. Check hdfs file system..')

    # now copying the file to hdfs
    ret, output, error = run_cmd(["hadoop", "fs", "-put", source, destination_dir])
    if ret != 0:
        return RuntimeError('Unable to copy file: ' + source + ' to hdfs location....\n Error: ' + error)
    else:
        return True


def run_cmd(args_list):
    logger.info('Running system command: {0}'.format(' '.join(args_list)))
    proc = Popen(args_list, stdout=PIPE, stderr=PIPE)
    s_output, s_err = proc.communicate()
    s_return = proc.returncode
    return s_return, s_output, s_err


def _check_if_present(list_of_files):
    list1 = [isfile(join(_SOURCE_DIR, f)) for f in list_of_files]

    # Checking if all the files in list1 exist or not...
    if all(list1):
        return True
    else:
        return False


def _get_feed_files(tracker_file_name):
    df = _validate_tracker_file(tracker_file_name)
    valid_feed_files = []
    invalid_feed_files = []
    for index, row in df.iterrows():
        _type, file_name = row[4], row[6]
        file_name_regex = str(_FEED_FILE_TYPE_NAME_MAP.get(_type))
        if file_name_regex:
            pattern = re.compile(file_name_regex)
            if pattern.match(file_name):
                valid_feed_files.append(file_name)
            else:
                invalid_feed_files.append(file_name)
        else:
            invalid_feed_files.append(file_name)

    return valid_feed_files, invalid_feed_files


def _validate_tracker_file(tracker_file_name):
    try:
        df = pd.read_csv(join(_SOURCE_DIR, tracker_file_name), sep=_CSV_FILE_DELIMITER)
    except Exception as e:
        raise RuntimeError('Failed to validate tracker file: ' + tracker_file_name +
                           '. Probably not a valid csv... ' + str(e))

    if _TRACKER_FILE_HEADER.split(',') != list(df.columns.values):
        raise RuntimeError('Invalid csv header in tracker file: ' + tracker_file_name + ' Leaving this tracker file...')

    return df


def _get_tracker_files():
    pattern = re.compile(_TRACKER_FILE_REGEX)
    try:
        tracker_files = [f for f in listdir(_SOURCE_DIR) if isfile(join(_SOURCE_DIR, f)) and pattern.match(f)]
    except Exception as e:
        raise RuntimeError('Failed to get the tracker files from the source directory...' + str(e))
    return tracker_files


if __name__ == '__main__':
    if sys.argv.__len__() < 4:
        logger.error('Expecting source directory, valid and invalid HDFS destination directory '
                     'as script arguments. Aborting now...')
        exit(-1)

    _SOURCE_DIR = sys.argv[1]
    _HDFS_DESTINATION_ROOT_DIR = sys.argv[2]
    _INVALID_FILES_HDFS_DIR = sys.argv[3]
    run()
