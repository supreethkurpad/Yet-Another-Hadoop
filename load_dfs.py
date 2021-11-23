import subprocess
import json
import random
import socket
import os
from sys import argv, stdout
from functools import reduce 
import pickle
from contextlib import closing
import shutil

DEFAULT_PORT=5000
NULL_FILE=os.devnull
HADOOP_HOME=os.environ.get('MYHADOOP_HOME','/home/swarupa/College/Sem5/Yet-Another-Hadoop/')

def getPortNumbers(n, blacklist=[]) -> list:
    ports = []
    while n:
        with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as a_socket:
            a_socket.bind(('', 0))
            a_socket.getsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            port = a_socket.getsockname()[1]
            if port not in blacklist:
                ports.append(port)
            else:
                continue
            n-=1
        
    return ports    

def delete(folder):
    for filename in os.listdir(folder):
        file_path = os.path.join(folder, filename)
        try:
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.unlink(file_path)
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)
        except Exception as e:
            print('Failed to delete %s. Reason: %s' % (file_path, e))
        
def prompt(config):
    # while(True):
    ch = input('Format DFS? [y/n]').lower()
    if ch == 'y':
        #format
        for dir in os.listdir(config["path_to_namenodes"]):
            delete(os.path.join(config["path_to_namenodes"], dir))
            os.mkdir(os.path.join(config['path_to_namenodes'], dir, config['fs_path']))
        for dir in os.listdir(config["path_to_datanodes"]):
            delete(os.path.join(config["path_to_datanodes"], dir))
    else:
        exit(0)

if __name__ == '__main__':
    

    if len(argv) < 2:
        print("Incorrect usage.Expected python3 start-all.py /path/to/dfs_setup_config ")
        exit(1)
    config_path = argv[1]
    
    # read config
    config = {}
    with open(config_path, 'r') as f:
        config = json.load(f) 

    if(config["num_loads"] == 0):
        prompt(config)

    config["num_loads"]+=1


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

    # assign ports to namenodes
    snn_port = getPortNumbers(1, blacklist=ports)[0]
    config['pnn_port'] = DEFAULT_PORT
    config['snn_port'] = snn_port
    
    namenode = os.path.join(HADOOP_HOME, 'src', 'servers', 'namenode.py')
    namenode_args = [DEFAULT_PORT, ports, config_path, True]

    # store the args as pickle objects at a path for namenode to parse
    argpath = os.path.join(HADOOP_HOME, 'tmp', 'namenode_arg.pickle')
    with open(argpath, 'wb') as f:
        pickle.dump(namenode_args, file=f)

    #add path to argpath
    config['path_to_argpath']=argpath
       
    # start namenode process
    namenode_process = subprocess.Popen(['python3', namenode, argpath])
    # log pid to tmp file so stop-all can terminate it.
    with open(os.path.join(HADOOP_HOME, 'tmp', 'pids.txt'), 'w+') as f:
       print(namenode_process.pid, file=f)

    # Repeating Process for Secondary NameNode
    s_namenode = os.path.join(HADOOP_HOME, 'src', 'servers', 'namenode.py')
    s_namenode_args = [snn_port, ports, config_path, True]
    s_argpath = os.path.join(HADOOP_HOME, 'tmp', 's_namenode_arg.pickle')
    with open(s_argpath, 'wb') as f:
        pickle.dump(s_namenode_args, file=f)

    #add path to argpath
    config['path_to_s_argpath']=argpath
    
    # start namenode process
    s_namenode_process = subprocess.Popen(['python3', namenode, s_argpath])
    # log pid to tmp file so stop-all can terminate it.
    with open(os.path.join(HADOOP_HOME, 'tmp', 'pids.txt'), 'a') as f:
       print(s_namenode_process.pid, file=f)


    # Starting datanodes
    numD = config['num_datanodes']

        
    datanode = os.path.join(HADOOP_HOME, 'src' , 'servers', 'datanode.py')
    pidD = []

    pid_file = os.path.join(HADOOP_HOME, "tmp", "pids.txt")
    config['pid_file'] = pid_file
    
    for i in range(numD):
        pass
        datanode_process = subprocess.Popen(['python3', datanode, str(i+1), str(ports[i]), str(config['block_size'])])
        with open(pid_file, 'a') as f:
            print(datanode_process.pid, file=f)

    with open(config_path,'w+') as f:
        json.dump(config, f)

    
    