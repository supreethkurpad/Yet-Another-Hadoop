import subprocess
import json
import os
from sys import argv, stdin, stdout
from functools import reduce 
import pickle
import shutil
from pathlib import Path
import sys

HADOOP_HOME=os.environ.get('MYHADOOP_HOME','/home/swarupa/College/Sem5/Yet-Another-Hadoop/')

if HADOOP_HOME not in sys.path:
    sys.path.insert(0, HADOOP_HOME)

from src.utils.port_finder import getPortNumbers
from src.utils.make_import_path import make_import_path

DEFAULT_PORT=5000
NULL_FILE=os.devnull
  
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


def dispConfig(config):
    print("Using config:\n", json.dumps(config, indent=4))


def main():
    if len(argv) < 2:
            config_path =  os.path.join(HADOOP_HOME, 'logs', 'previous_config.json')
            if os.path.getsize(config_path) == 0:
                print("Incorrect Usage. Please specify the config path")
                exit(1)
    else:
        config_path = argv[1]
    
    # read config
    config = {}
    with open(config_path, 'r') as f:
        config = json.load(f)

    if(config["num_loads"] == 0):
        prompt(config)

    config["num_loads"]+=1

    dispConfig(config)

    with open(os.path.join(HADOOP_HOME, 'logs', 'previous_config.json'), 'w') as f:
        json.dump(config, f)

    # set up logging paths. writes to /dev/null by default 
    DN_LOG_FILE = config.get('datanode_log_path', NULL_FILE)
    NN_LOG_FILE = config.get('namenode_log_path', NULL_FILE)

    if DN_LOG_FILE is not NULL_FILE:
        DN_LOG_FILE = os.path.join(DN_LOG_FILE, 'logs.txt')

    if NN_LOG_FILE is not NULL_FILE:
        NN_LOG_FILE = os.path.join(NN_LOG_FILE, 'logs.txt')

    NN_LOG_FILE = open(NN_LOG_FILE, 'w')
    DN_LOG_FILE = open(DN_LOG_FILE, 'w')

    # ports to open datanodes on
    ports = getPortNumbers(config['num_datanodes'])
    with open(os.path.join(HADOOP_HOME,'tmp','ports.txt'),'w+') as f:
        config['path_to_ports']=os.path.join(HADOOP_HOME,'tmp','ports.txt')
        for i in ports:
            print(i,file=f)

    # assign ports to namenodes
    pnn_port, snn_port = getPortNumbers(2, blacklist=ports)
    config['pnn_port'] = pnn_port
    config['snn_port'] = snn_port
    
    namenode = make_import_path(os.path.relpath(os.path.join(HADOOP_HOME, 'src', 'servers', 'namenode.py')))
    namenode_name = os.path.basename(config['path_to_primary'])
    namenode_args = [pnn_port, ports, config_path, True, namenode_name]

    # store the args as pickle objects at a path for namenode to parse
    argpath = os.path.join(HADOOP_HOME, 'tmp', 'namenode_arg.pickle')
    with open(argpath, 'wb') as f:
        pickle.dump(namenode_args, file=f)

    #add path to argpath
    config['path_to_argpath']=argpath
    
    # start namenode process

    namenode_process = subprocess.Popen(['python3', "-m", namenode, argpath], stdout=NN_LOG_FILE, stderr=NN_LOG_FILE)
    # log pid to tmp file so stop-all can terminate it.
    with open(os.path.join(HADOOP_HOME, 'tmp', 'pids.txt'), 'w+') as f:
        print(namenode_process.pid, file=f)

    # Repeating Process for Secondary NameNode
    s_namenode = make_import_path(os.path.relpath(os.path.join(HADOOP_HOME, 'src', 'servers', 'namenode.py')))
    s_namenode_name = os.path.basename(config['path_to_secondary'])
    s_namenode_args = [snn_port, ports, config_path, False, s_namenode_name]
    s_argpath = os.path.join(HADOOP_HOME, 'tmp', 's_namenode_arg.pickle')
    with open(s_argpath, 'wb') as f:
        pickle.dump(s_namenode_args, file=f)

    #add path to argpath
    config['path_to_s_argpath']=argpath
    
    # start namenode process
    s_namenode_process = subprocess.Popen(['python3', '-m', s_namenode, s_argpath], stdout=NN_LOG_FILE, stderr=NN_LOG_FILE)
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
        datanode_process = subprocess.Popen(['python3', datanode, str(i+1), str(ports[i]), str(config['block_size'])], stdout=DN_LOG_FILE, stderr=DN_LOG_FILE)
        with open(pid_file, 'a') as f:
            print(datanode_process.pid, file=f)

    with open(config_path,'w+') as f:
        json.dump(config, f)

    cli = subprocess.run(["python3", "-m", "src.cli.client", config_path], stdin=stdin)

if __name__ == '__main__':
    
    try:
        main()    
    except KeyboardInterrupt as e:
        pass

    print("\nShutting down all namenodes, datanodes and client")