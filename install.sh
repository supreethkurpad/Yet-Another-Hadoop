python -m venv $MYHADOOP_HOME/hadoop_env
source $MYHADOOP_HOME/hadoop_env/bin/activate
pip install -r $MYHADOOP_HOME/requirements.txt
cd $MYHADOOP_HOME
mkdir directories
mkdir directories/namenodes
mkdir directories/datanodes
mkdir tmp
mkdir configs
touch configs/config.json configs/dfs_setup_config.json
echo "Place your config files into configs folder"
