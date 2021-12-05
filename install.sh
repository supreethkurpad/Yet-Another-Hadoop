#!/bin/bash

python -m venv $MYHADOOP_HOME/hadoop_env
chmod +x  $MYHADOOP_HOME/hadoop_env/bin/activate
. $MYHADOOP_HOME/hadoop_env/bin/activate
pip install -r $MYHADOOP_HOME/requirements.txt
cd $MYHADOOP_HOME
mkdir directories
mkdir directories/namenodes
mkdir directories/datanodes
mkdir tmp
mkdir configs
echo  '{
    "block_size": 4096,
    "path_to_datanodes": "/home/suvigya/PythonCode/Yet-Another-Hadoop/directories/datanodes",
    "path_to_namenodes": "/home/suvigya/PythonCode/Yet-Another-Hadoop/directories/namenodes",
    "replication_factor": 3,
    "num_datanodes": 4,
    "datanode_size": 10240,
    "sync_period": 10,
    "datanode_log_path": "/home/suvigya/PythonCode/Yet-Another-Hadoop/directories/datanodes/logs",
    "namenode_log_path": "/home/suvigya/PythonCode/Yet-Another-Hadoop/directories/namenodes/logs",
    "namenode_checkpoints": "/home/suvigya/PythonCode/Yet-Another-Hadoop/directories/checkpoints",
    "fs_path": "user/",
    "dfs_setup_config": "/home/suvigya/PythonCode/Yet-Another-Hadoop/configs/dfs_setup_config.json"
}' | cat >> configs/config.json 
