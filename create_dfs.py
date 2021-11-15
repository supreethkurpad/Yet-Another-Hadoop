from sys import argv
import json
import os

HADOOP_HOME=os.environ.get('MYHADOOP_HOME','/home/suvigya/PythonCode/Yet-Another-Hadoop/')

def checkNameNode(config):
    """
        Read the paths to namenodes from config
        If no directory exists just create new directories
        If a directory exists, use existing namenodes and datanodes without formatting.
        Write details to the dfs_setup_config
    """
    p_namenode = os.path.join(config['path_to_namenodes'], 'primary')
    s_namenode = os.path.join(config['path_to_namenodes'], 'secondary')
    try:
        os.mkdir(p_namenode)
        os.mkdir(s_namenode)

    except FileExistsError as e:
        print("Namenodes already exist. Using existing namenode, might require format...")
    

    with open(config['dfs_setup_config'], 'w+') as f:

        dfs_config = config.copy()
        dfs_config['num_loads'] = 0
        dfs_config["path_to_primary"] = p_namenode
        dfs_config["path_to_secondary"] = s_namenode
   
        json.dump(dfs_config, f)
        
def checkDataNodes(config):
    """
        Read the path to datanodes from config
        If no directory exists throw error
        Write details to the dfs_setup_config
    """
    path_to_datanodes = config['path_to_datanodes']
    
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
    