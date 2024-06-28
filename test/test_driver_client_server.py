#!/usr/bin/env python3

import sys
import subprocess
import os

def wait_for_completion(procname, proc, wait_time):
    try:
        outs, err = proc.communicate(timeout=wait_time)
    except:
        proc.terminate()
        outs, err = proc.communicate()

    print("{} Return Code: {}".format(procname, proc.returncode))
    print("stdout:\n{}".format(outs.decode("utf-8")))
    print("stderr:\n{}".format(err.decode("utf-8")))

    return proc.returncode, outs, err

if __name__ == '__main__':
    errors = 0
    # Launch the server then the client
    server = subprocess.Popen(['./test_client_server', '--server'], env=dict(os.environ), stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=False)
    client = subprocess.Popen(['./test_client_server', '--client'], env=dict(os.environ), stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=False)

    # Wait for the client then the server to finish
    client_ecode, client_out, client_err = wait_for_completion("axl_client", client, 30)
    server_ecode, server_out, server_err = wait_for_completion("axl_server", server, 2)

    if server_ecode != 0 or client_ecode != 0:
        errors = server_ecode + client_ecode

    sys.exit(errors)
