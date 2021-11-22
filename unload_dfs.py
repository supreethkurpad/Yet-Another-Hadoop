import os
from signal import SIGTERM

HADOOP_HOME = os.environ.get('MYHADOOP_HOME','/home/suvigya/PythonCode/Yet-Another-Hadoop/')

tmp = open(os.path.join(HADOOP_HOME, 'tmp', 'pids.txt'), 'r')
for child in tmp.read().split('\n'):
    if child is None or child == "": break
    print("Killing process", child)
    try:
        os.kill(int(child), SIGTERM)
    except:
        print("process not found")
tmp.close()