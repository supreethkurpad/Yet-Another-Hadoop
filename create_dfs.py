from sys import argv
import json
import os

HADOOP_HOME=os.environ.get('MYHADOOP_HOME','/home/swarupa/College/Sem5/Yet-Another-Hadoop/')

def checkNameNode(config):
    """
        Read the paths to namenodes from config
        If no directory exists just create new directories
        If a directory exists, use existing namenodes and datanodes without formatting.
        Write details to the dfs_setup_config
    """
    if not (os.path.exists(config['path_to_namenodes'])):
        print("Invalid directory for path to namenodes, does not exist")

    p_namenode = os.path.join(config['path_to_namenodes'], 'namenode1')
    s_namenode = os.path.join(config['path_to_namenodes'], 'namenode2')
    usr = os.path.join(p_namenode, config["fs_path"])
    try:
        os.mkdir(p_namenode)
        os.mkdir(s_namenode)
        os.mkdir(usr)
    
    

    except FileExistsError as e:
        print("Namenodes already exist. Using existing namenode, might require format...")
    
    if(os.path.exists(config['dfs_setup_config'])):
        with open(config['dfs_setup_config'], 'w+') as f:

            dfs_config = config.copy()
            dfs_config['num_loads'] = 0
            dfs_config["path_to_primary"] = p_namenode
            dfs_config["path_to_secondary"] = s_namenode
    
            json.dump(dfs_config, f)
    else:
        print("Path to setup config file invalid")

    if not (os.path.exists(config['namenode_log_path'])):
        os.mkdir(config['namenode_log_path'])
        
def checkDataNodes(config):
    """
        Read the path to datanodes from config
        If no directory exists throw error
        Write details to the dfs_setup_config
    """
    path_to_datanodes = config['path_to_datanodes']
    list_of_datanode_paths=[]
    if(config['num_datanodes']>0):
        for i in range(config['num_datanodes']):
            data_node = os.path.join(config['path_to_datanodes'], 'datanode'+str(i+1))
            #print(data_node)
            list_of_datanode_paths.append(data_node)
            try:
                os.mkdir(data_node)
            except FileExistsError as e:
                print("Datanodes already exist. Using existing datanode, might require format...")
    else:
        print("number of datanodes not valid")

    with open(config['dfs_setup_config'], 'w+') as f:
        dfs_config = config.copy()
        dfs_config['path_to_each_datanode'] = list_of_datanode_paths
        json.dump(dfs_config, f)


    if not (os.path.exists(config['datanode_log_path'])):
        os.mkdir(config['datanode_log_path'])

    if not os.path.exists(path_to_datanodes):
        print("Invalid directory for datanodes. Does not exist")
        exit(1)


if __name__ == '__main__':
    
    if len(argv) < 2:
        config_path = os.path.join(HADOOP_HOME, 'configs', 'config.json')
        print("Using default config...")
    else:
        config_path = argv[1]
    

    # read config
    config = {}
    with open(config_path, 'r') as f:
        config = json.load(f) 

    print(f"Creating DFS...")
    # Check datanode related configs 
    checkDataNodes(config)

    # check namenode related configs
    checkNameNode(config)
    