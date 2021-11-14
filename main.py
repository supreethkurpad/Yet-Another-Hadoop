from sys import argv
import json
from typing import NamedTuple 
from src.servers.namenode import NameNode
import socket
import random
import multiprocessing
import os

DEFAULT_PORT=5000

def createNameNode(config):
    """
        Read the paths to namenodes and datanodes from config
        If no directory exists just create new directories and a new dfs_setup_config file.
        If a directory exists:
            -> Check if config matches directory. If yes use the existing setup
            -> Else prompt user to format namenodes AND datanodes, then do it if user agrees.
    """
    try:
        p_namenode = os.path.join(config['path_to_namenodes'], 'primary')
        s_namenode = os.path.join(config['path_to_namenodes'], 'secondary')
        os.mkdir(p_namenode)
        os.mkdir(s_namenode)

    except Exception as e:
        print("FATAL error in main.py/createNameNode: Could not create namenode", e)
        # exit(2)
    
            
    f = open(os.path.join(p_namenode, "fsimage.json"), 'w+')
    f.close()

    f = open(config['dfs_setup_config'], 'w')
    print(config, p_namenode, s_namenode,file=f)
    f.close()
        
def createDataNodes(config):
    """
        Read the paths to namenodes and datanodes from config
        If no directory exists just create new directories and a new dfs_setup_config file.
        If a directory exists:
            -> Check if config matches directory. If yes use the existing setup
            -> Else prompt user to format namenodes AND datanodes, then do it if user agrees.
    """
    path_to_datanode = config['path_to_datanodes']
    paths = []
    for i in range(config['num_datanodes']):
        fname = os.path.join(path_to_datanode, f'datanode_{i+1}.dat')
        paths.append(fname)
        open(fname, 'w+')

    f = open(config['dfs_setup_config'], 'a+')
    print(paths,file=f)
    f.close()

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
    try:
        if len(argv) < 2:
            print("python main.py /path/to/config ")
            exit(1)
        config_path = argv[1]
        
        # read config
        config = {}
        with open(config_path, 'r') as f:
            config = json.load(f)
        
        
        # prepare the directories, format where needed (TODO) 
        createNameNode(config)
        createDataNodes(config)

        # Start Datanodes
        ports = getPortNumbers(config['num_datanodes'])
        for port in ports:
            print(port)
            # TODO start a datanode at `port`

        # Start Namenode
        # TODO make logs go to a different file
        name_node = multiprocessing.Process(target=NameNode, args=(DEFAULT_PORT, config_path, True, ports))
        name_node.start()
        # NameNode(DEFAULT_PORT, path_to_config=config_path, primary=False)
    except KeyboardInterrupt:
        print("Shutting down all nodes...")