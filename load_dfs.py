import subprocess
import json
import random
import socket
import os
from sys import argv, stdout
from functools import reduce 
import pickle

DEFAULT_PORT=5000
NULL_FILE=os.devnull
HADOOP_HOME=os.environ.get('MYHADOOP_HOME','/home/suvigya/PythonCode/Yet-Another-Hadoop/')

def getPortNumbers(n) -> list:
    ports = []
    while n:
        port = random.randint(1024, 65535)
        location = ("127.0.0.1", port)
        a_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        result_of_check = a_socket.connect_ex(location)

        if result_of_check == 0:
            n -= 1
            ports.append(port)
        else:
            continue
    
    return ports    

if __name__ == '__main__':
    

    if len(argv) < 2:
        print("Incorerct usage.Expected python start-all.py /path/to/dfs_setup_config ")
        exit(1)
    config_path = argv[1]
    

    # read config
    config = {}
    with open(config_path, 'r') as f:
        config = json.load(f) 

    # set up logging paths. writes to /dev/null by default 
    DN_LOG_FILE = config.get('datanode_log_path', NULL_FILE)
    NN_LOG_FILE = config.get('namenode_log_path', NULL_FILE)
    
    NN_LOG_FILE = open(NN_LOG_FILE, 'w')
    DN_LOG_FILE = open(DN_LOG_FILE, 'w')

    # ports to open datanodes on
    ports = getPortNumbers(config['num_datanodes'])

    namenode = os.path.join(HADOOP_HOME, 'src', 'servers', 'namenode.py')
    namenode_args = [DEFAULT_PORT, ports, config_path, True]

    # store the args as pickle objects at a path for namenode to parse
    argpath = os.path.join(HADOOP_HOME, 'tmp', 'namenode_arg.pickle')
    with open(argpath, 'wb') as f:
        pickle.dump(namenode_args, file=f)

    # start namenode process
    namenode_process = subprocess.Popen(['python', namenode, argpath], stderr=stdout, stdout=NN_LOG_FILE)
    
    # log pid to tmp file so stop-all can terminate it.
    with open(os.path.join(HADOOP_HOME, 'tmp', 'pids.txt'), 'a+') as f:
        print(namenode_process.pid, file=f)

    # TODO start datanode processes