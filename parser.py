import argparse
import sys
import json
import os
import requests

HADOOP_HOME=os.environ.get('MYHADOOP_HOME','/home/swarupa/College/Sem5/Yet-Another-Hadoop/')

parser = argparse.ArgumentParser()
parser.add_argument("-i", "--input", type=str, dest="input")
parser.add_argument("-o", "--output", type=str, dest="output")
parser.add_argument("-c", "--config", type=str, dest="config")
parser.add_argument("-m", "--mapper", type=str, dest="mapper")
parser.add_argument("-r", "--reducer", type=str, dest="reducer")

args = parser.parse_args()
# print(args)

with open(os.path.join(HADOOP_HOME, 'configs', 'dfs_setup_config.json'), 'r') as f:
    config = json.load(f)
with open(config['path_to_ports'], 'r') as f:
    dnn_ports = f.read().splitlines()

port = config['pnn_port']

if (requests.head('http://localhost:'+str(port))).status_code==200:
    ffile = requests.post("http://localhost:"+str(port)+"/cat", json={"fspath" : args.input}).json()
else:
    print(requests.head('http://localhost:'+str(port)).status_code)

if ffile['code']== '1':
    # print("no file path")
    exit(0)
elif ffile['code'] == '0':
    dnodes = ffile['data'].split('\n')
    for dn in dnodes.rstrip():
        if(dn!=""):
            dnode_dict = dict(map(lambda x: x.split(','), dn.split(' ')))
            for p in dnode_dict.keys():
                data=requests.get('http://localhost:'+str(dnn_ports[int(p)-1])+'/read/'+str(dnode_dict[p]))
                dd =data.json()
                if(data is None):
                    print("datanode down")
                else:
                    # print(dd['data']) 
                    break
