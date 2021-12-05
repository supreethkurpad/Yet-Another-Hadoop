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
    "path_to_datanodes": "'$MYHADOOP_HOME'/directories/datanodes",
    "path_to_namenodes": "'$MYHADOOP_HOME'/directories/namenodes",
    "replication_factor": 3,
    "num_datanodes": 4,
    "datanode_size": 10240,
    "sync_period": 10,
    "datanode_log_path": "'$MYHADOOP_HOME'/directories/datanodes/logs",
    "namenode_log_path": "'$MYHADOOP_HOME'/directories/namenodes/logs",
    "namenode_checkpoints": "'$MYHADOOP_HOME'/directories/checkpoints",
    "fs_path": "user/",
    "dfs_setup_config": "'$MYHADOOP_HOME'/configs/dfs_setup_config.json"
}' | cat >> configs/config.json 
