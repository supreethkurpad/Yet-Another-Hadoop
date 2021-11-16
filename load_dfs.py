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
    
    #open()

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
    #namenode_process = subprocess.Popen(['python', namenode, argpath], stderr=stdout, stdout=NN_LOG_FILE)
    
    # log pid to tmp file so stop-all can terminate it.
    #with open(os.path.join(HADOOP_HOME, 'tmp', 'pids.txt'), 'a+') as f:
    #    print(namenode_process.pid, file=f)

    argpathD = os.path.join(HADOOP_HOME, 'tmp', 'datanode_arg.pickle')

    #add path to datanode argpath
    config['path_to_datanode_argpath']=argpathD

    numD = config['num_datanodes']

        
    datanode = os.path.join(HADOOP_HOME, 'src' , 'servers', 'datanode.py')
    pidD = []

    open(os.path.join(HADOOP_HOME, 'tmp', 'datanodespid.txt'), 'w').close()
    
    for i in range(numD):
        datanode_args = (i+1, ports[i], config['block_size'])
        with open(argpath, 'w') as f:
            pickle.dump(datanode_args, f)
        datanode_process = subprocess.Popen(['python', datanode, argpathD], stderr=stdout, stdout=DN_LOG_FILE)
        pidD.append(datanode_process.pid)
        with open(os.path.join(HADOOP_HOME, 'tmp', 'datanodespid.txt'), 'a+') as f:
            print(datanode_process.pid, file=f)
