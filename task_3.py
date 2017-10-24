""" Detect local mounted disk (make sure it is local) with at least X MB free space,
create Z files of size Y, run Z “dd” processes which where each process will fill selected file
with Data and print time took to complete the work. """

"""
RUN:   python task_3.py -x 1000 -y 10 -z 3 -d "0b1"
"""

import subprocess
import argparse
import traceback
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed


def parseArgs():
    """Defines arguments and parsing routines"""
    # TODO Need some check to verify arguments
    parser = argparse.ArgumentParser()

    parser.add_argument('-x', dest='min_space', required=True)
    parser.add_argument('-y', dest='file_size', required=True)
    parser.add_argument('-z', dest='number_files', required=True)
    parser.add_argument('-d', dest='data', required=False, default="0b0")
    return parser.parse_args()


def runcmd(cmd):
    return subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, universal_newlines=True).stdout


def get_full_command(disk, filename):
    file_path = os.path.join(disk, "tmp", filename)
    return create_file_pattern.format(file_path, file_size)


if __name__ == "__main__":

    start_time = time.time()

    args = parseArgs()

    assert args.min_space
    assert args.file_size
    assert args.number_files
    assert args.data

    min_space = int(args.min_space)
    file_size = int(args.file_size)
    number_files = int(args.number_files)
    data = args.data

    get_disks_cmd = """ df | grep "^/dev" | awk '{ print $4, $6 }' """
    create_file_pattern = "dd if=/dev/zero of={} bs={} count=1"

    output = runcmd(get_disks_cmd)

    disks = []

    for disk_data in output.splitlines():

        try:
            free_space, path = disk_data.split()

            print("\nDisk mounted on path '{}' has '{}' bytes of free space".format(path, free_space))
            if int(free_space) >= min_space:
                disks.append(path)
                print("Disk has enough of free space")
            else:
                print("Disk hasn't enough of free space")


        except Exception as e:
            tb = traceback.format_exc()
            print(tb)

    file_names = ("file" + str(i) for i in range(number_files))
    for disk in disks:

        with ThreadPoolExecutor(max_workers=200) as pool:
            results = [pool.submit(runcmd, get_full_command(disk, name)) for name in file_names]

            for future in as_completed(results):
                print(future.result())

    elapsed_time = time.time() - start_time

    print("Process duration:", elapsed_time)
