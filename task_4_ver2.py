""" Run user-selected command on many servers (user provided as param) with ssh in parallel,
collect output from all nodes. Script should print collected output from all nodes on stdout,
w/o using temp files. """

"""
RUN: python task_4.py -c ifconfig
"""

import argparse
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import paramiko
from socket import timeout, gaierror


def parseArgs():
    """Defines arguments and parsing routines"""
    # TODO Need some check to verify arguments

    parser = argparse.ArgumentParser()
    parser.add_argument('-c', dest='command', default="netstat -nr", required=False)

    return parser.parse_args()


def runremote(cmd, host, user, password):
    """

    :param cmd:
    :param host:
    :param user:
    :param password:
    :return:
    """
    assert cmd
    assert host
    assert user
    assert password

    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    try:
        ssh.connect(host, username=user, password=password, timeout=10)
        stdin, stdout, stderr = ssh.exec_command(cmd)
        out = stdout.readlines()
        error = stderr.readlines()
    except paramiko.ssh_exception.AuthenticationException as auth_e:
        out, error = ["AuthenticationException"], ["AuthenticationError"]

    except timeout:
        out, error = ["timeout Exception"], ["socket.timeout: timed out"]

    except gaierror:
        out, error = ["gaierror Exception"], ["socket.gaierror: Name or service not known"]

    finally:
        ssh.close()

    if out:
        for line in out:
            print("HOST: [{}] - OUT - {}".format(host, line.strip()))
    if error:
        for line in error:
            print("HOST: [{}] - ERROR - {}".format(host, line.strip()))




    return host, out, error


if __name__ == "__main__":

    args = parseArgs()

    srv1 = "127.0.0.1", "user1", "pwd",
    srv2 = "1227.0.0.1", "user2", "pwd",

    servers = srv1, srv2

    start_time = datetime.now()

    with ThreadPoolExecutor(max_workers=200) as pool:
        results = [pool.submit(runremote, args.command, *srv) for srv in servers]

    tdelta = datetime.now() - start_time

    print("\nTime total in multithread:", tdelta)
