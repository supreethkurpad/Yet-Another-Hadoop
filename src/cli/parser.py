import argparse
import sys
import json
import os
import requests
import os
import subprocess
import math

def partition(metadata,config):
    with open(config['path_to_ports'],'r') as f:
        ports=f.read().splitlines()
        
    with open('tmp/temp.txt', 'rb') as f: 
        dnodes=metadata.split('\n')
        for dn in dnodes:
            dn=dn.rstrip()
            if(dn!=""):
                dnode_dict = dict(map(lambda x: x.split(','), dn.split(' ')))
                chunk = f.read(config['block_size'])
                req_data={'data': chunk.decode('utf-8')}
                dns = list(dnode_dict.keys())
                for i in range(config['replication_factor']):
                    req_data[ports[int(dns[i])-1]] = dnode_dict[dns[i]]
                final_res=post('',ports[int(dns[0])-1],'write',req_data)
        return final_res

def post(cpath,port,cmd,data):
        try:
            if((requests.head('http://localhost:'+str(port))).status_code==200):
                try:
    
                    res=requests.post('http://localhost:'+str(port)+'/'+str(cmd),json=data) 
                    return res
                except Exception as e:
                        print(e)
        except:
            sleep(15)
            with open(os.path.join(HADOOP_HOME, cpath), 'r') as f:
                config = json.load(f)
            p_port=self.config['pnn_port']
            if((requests.head('http://localhost:'+str(p_port))).status_code==200):
                try:
                    res=requests.post('http://localhost:'+str(p_port)+'/'+str(cmd),json=data) 
                    return res
                except Exception as e:
                        print(e)

def putresult(cpath,oppath,opfile):
    with open(os.path.join(HADOOP_HOME, args.config), 'r') as f:
        config = json.load(f)
    file_size = os.path.getsize('tmp/temp.txt')
    res = post(cpath,config['pnn_port'],'put',{"filepath":opfile,"path_in_fs":oppath,\
            "size":math.ceil(file_size/config['block_size'])})
    res=res.json()
    if(res['code']!='0'):
        print("Error occured with status and description ",res['code']," - ",res['error'])
        return "error"
        #recieves file
    final_res =partition(res['data'],config)
    return "success"


HADOOP_HOME=os.environ.get('MYHADOOP_HOME','/home/swarupa/College/Sem5/Yet-Another-Hadoop/')

parser = argparse.ArgumentParser()
parser.add_argument("-i", "--input", type=str, dest="input")
parser.add_argument("-o", "--output", type=str, dest="output")
parser.add_argument("-c", "--config", type=str, dest="config")
parser.add_argument("-m", "--mapper", type=str, dest="mapper")
parser.add_argument("-r", "--reducer", type=str, dest="reducer")

args = parser.parse_args()

with open(os.path.join(HADOOP_HOME, args.config), 'r') as f:
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
    for dn in dnodes:
        dn=dn.rstrip()
        if(dn!=""):
            dnode_dict = dict(map(lambda x: x.split(','), dn.split(' ')))
            for p in dnode_dict.keys():
                data=requests.get('http://localhost:'+str(dnn_ports[int(p)-1])+'/read/'+str(dnode_dict[p]))
                dd =data.json()
                if(data is None):
                    print("datanode down")
                else:
                    cmd = "python "+args.mapper+" | sort | python "+args.reducer
                    ps = subprocess.Popen(cmd,shell=True,stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
                    ps.stdin.write(bytes(dd['data'], 'utf-8'))
                    output = ps.communicate()[0]
                    result = output.decode()
                    with open('tmp/temp.txt','a') as f:
                        f.write(result)
    res=putresult(args.config,args.output,'output.txt')
    with open('tmp/temp.txt','w') as f:
                        f.write(res)
    res2=putresult(args.config,args.output,'status.txt')

                    
