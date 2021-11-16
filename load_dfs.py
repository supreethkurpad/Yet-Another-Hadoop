import subprocess
import json
import random
import socket
import os
from sys import argv, stdout
from functools import reduce 
import pickle
from contextlib import closing

DEFAULT_PORT=5000
NULL_FILE=os.devnull
HADOOP_HOME=os.environ.get('MYHADOOP_HOME','/home/swarupa/College/Sem5/Yet-Another-Hadoop/')

def getPortNumbers(n) -> list:
    ports = []
    while n:
        with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as a_socket:
            a_socket.bind(('', 0))
            a_socket.getsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            ports.append(a_socket.getsockname()[1])
            n-=1
        
    return ports    

if __name__ == '__main__':
    

    if len(argv) < 2:
        print("Incorerct usage.Expected python3 start-all.py /path/to/dfs_setup_config ")
        exit(1)
    config_path = argv[1]
    
    # read config
    config = {}
    with open(config_path, 'r') as f:
        config = json.load(f) 

    # set up logging paths. writes to /dev/null by default 
    #DN_LOG_FILE = config.get('datanode_log_path', NULL_FILE)
    #NN_LOG_FILE = config.get('namenode_log_path', NULL_FILE)
    DN_LOG_FILE =NULL_FILE
    NN_LOG_FILE =NULL_FILE
    NN_LOG_FILE = open(NN_LOG_FILE, 'w')
    DN_LOG_FILE = open(DN_LOG_FILE, 'w')

    # ports to open datanodes on
    ports = getPortNumbers(config['num_datanodes'])
    with open(os.path.join(HADOOP_HOME,'tmp','ports.txt'),'w+') as f:
        config['path_to_ports']=os.path.join(HADOOP_HOME,'tmp','ports.txt')
        for i in ports:
            print(i,file=f)

    
    namenode = os.path.join(HADOOP_HOME, 'src', 'servers', 'namenode.py')
    namenode_args = [DEFAULT_PORT, ports, config_path, True]

    # store the args as pickle objects at a path for namenode to parse
    argpath = os.path.join(HADOOP_HOME, 'tmp', 'namenode_arg.pickle')
    with open(argpath, 'wb') as f:
        pickle.dump(namenode_args, file=f)

    #add path to argpath
    config['path_to_argpath']=argpath
    

    # start namenode process
    namenode_process = subprocess.Popen(['python3', namenode, argpath], stderr=stdout)
    
    # log pid to tmp file so stop-all can terminate it.
    with open(os.path.join(HADOOP_HOME, 'tmp', 'pids.txt'), 'w+') as f:
       print(namenode_process.pid, file=f)

    numD = config['num_datanodes']

        
    datanode = os.path.join(HADOOP_HOME, 'src' , 'servers', 'datanode.py')
    pidD = []

    pid_file = os.path.join(HADOOP_HOME, "tmp", "pids.txt")
    open(os.path.join(HADOOP_HOME, 'tmp', 'datanodespid.txt'), 'w').close()

    with open('start-datanodes.sh', 'w') as f:
        for i in range(numD):
            datanode_process = subprocess.Popen(['python3', datanode, str(i+1), str(ports[i]), str(config['block_size'])], stderr=stdout)
            with open(pid_file, 'a') as f:
                print(datanode_process.pid, file=f)
    
    with open(config_path,'w') as f:
        json.dump(config,file=f)
    
    