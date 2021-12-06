import os
import subprocess
import requests
import sys
import json
import urllib.request
import math
from time import sleep

from termcolor import colored

HADOOP_HOME=os.environ.get('MYHADOOP_HOME','/home/swarupa/College/Sem5/Yet-Another-Hadoop/')
COLOR="cyan"
TERM_ROWS=50
class Client:
    def __init__(self,config_path):
        self.config_path= config_path
        self.config=self.getconfig()
        self.ports=self.getports()
        self.p_port=self.config['pnn_port']
        self.params=None
        self.snn_port = self.config['snn_port']

    def getconfig(self):
        config = {}
        with open(self.config_path, 'r') as f:
            config = json.load(f)
        return config
    
    def getports(self):
        with open(self.config['path_to_ports'],'r') as f:
            ports=f.read().splitlines()
        return ports




    def post(self,port,cmd,data):
        try:
            if((requests.head('http://localhost:'+str(port))).status_code==200):
                try:
    
                    res=requests.post('http://localhost:'+str(port)+'/'+str(cmd),json=data) 
                    return res
                except Exception as e:
                        print(e)
        except:
            while True:
                sleep(self.config['sync_period'])
                self.config=self.getconfig()
                self.p_port=self.config['pnn_port']
                if((requests.head('http://localhost:'+str(self.p_port))).status_code==200):
                    try:
                        res=requests.post('http://localhost:'+str(self.p_port)+'/'+str(cmd),json=data) 
                        return res
                    except Exception as e:
                            print(e)



    def partition(self,metadata):
        
        with open(self.params[1], 'rb') as f:
            
            dnodes=metadata.split('\n')
            for dn in dnodes:
                dn=dn.rstrip()
                if(dn!=""):
                    dnode_dict = dict(map(lambda x: x.split(','), dn.split(' ')))
                    chunk = f.read(self.config['block_size'])
                    req_data={'data': chunk.decode('utf-8')}
                    dns = list(dnode_dict.keys())
                    for i in range(self.config['replication_factor']):
                        req_data[self.ports[int(dns[i])-1]] = dnode_dict[dns[i]]

                    final_res=self.post(self.ports[int(dns[0])-1],'write',req_data)
        return final_res
        

    def sendputRequest(self):
        res=None
        if(self.params is None):
            print("Enter file path")
            return
        if not os.path.exists(self.params[1]):
            print("File does not exist in path specified")
            return

        file_size = os.path.getsize(self.params[1])
        res = self.post(self.p_port,self.params[0],{"filepath":self.params[1],"path_in_fs":self.params[2],\
            "size":math.ceil(file_size/self.config['block_size'])}) 
        res=res.json()
        if(res['code']!='0'):
            print("Error occured with status and description ",res['code']," - ",res['error'])
            return
        #recieves file
        final_res =self.partition(res['data'])
        final_res=final_res.json()
        #if(final_res==None):
           # print("failed to insert file")
        print("File inserted successfully")

    def getfileblocks(self,res):
        res=res.json()
        if(res['code']!='0'):
            print("Error occured with status and description ",res['code']," - ",res['error'])
            return
        dnodes=res['data'].split('\n')
        for dn in dnodes:
            dn=dn.rstrip()
            if(dn!=""):
                dnode_dict = dict(map(lambda x: x.split(','), dn.split(' ')))
                for p in dnode_dict.keys():
                    try:
                        data=requests.get('http://localhost:'+str(self.ports[int(p)-1])+'/read/'+str(dnode_dict[p]))
                        d=data.json()
                        if(data is None):
                            print("datanode down")
                            return
                        else:
                            print(d['data'])
                            break
                    except:
                        print("datanode down")

            
    def delblocks(self,metadata):
        dnodes=metadata.strip().split('\n')
        for dn in dnodes:
            dn=dn.rstrip()
            if(dn!=""):
                dnode_dict = dict(map(lambda x: x.split(','), dn.split(' ')))
                req_data={}
                dns = list(dnode_dict.keys())
                for i in range(int(self.config['replication_factor'])):
                    req_data[self.ports[int(dns[i])-1]] = dnode_dict[dns[i]]

                final_res=self.post(self.ports[int(dns[0])-1],'delete',req_data)
        print("Deleted successfully")

    

    def startReqHandler(self):
        while True:
        
            req=input(colored("\nyah> ", COLOR))
            #put file /dir 
            self.params=req.strip().split(" ")

            try:
                if(self.params[0]=='exit'):
                    unload()
                    break

                if(self.params[0]=='put'):
                    if len(self.params)<2:
                        print("Incorrect number of parameters, enter file path and hdfs dir")
                        continue
                    if len(self.params) == 2:
                        self.params.append(self.config['fs_path'])

                    self.sendputRequest()
                    

                elif(self.params[0]=='mkdir'):
                    if len(self.params)!=2:
                        print('Incorrect number of parameters, enter name of one directory')
                        continue
                    res=self.post(self.p_port, self.params[0], {'fspath':self.params[1]})
                    res=res.json()
                    if(res['code']!='0'):
                        print(res['error'])
                        continue
                    print(res['msg']) 
                
                # Delete file or directory
                elif(self.params[0]=='rm'): 
                    if len(self.params)!=2:
                        print('Incorrect number of parameters, enter name file or directory')
                        continue
                    res=self.post(self.p_port, self.params[0], {'fspath':self.params[1]})
                    res=res.json()
                    if(res['code']!='0'):
                        print(res['error'])
                        continue
                    self.delblocks(res['data'])
                    
                
                elif(self.params[0]=='rmdir'):
                    if len(self.params)!=2:
                        print('Incorrect number of parameters, enter name of one directory')
                        continue
                    res=self.post(self.p_port, self.params[0], {'fspath':self.params[1]})
                    res=res.json()
                    if(res['code']!='0'):
                        print(res['error'])
                        continue
                    print(res['msg']) 

                elif(self.params[0]=='ls'):
                    res=None
                    req_param=self.config['fs_path']

                    if len(self.params) == 1:
                        self.params.append(self.config['fs_path'])

                    if(len(self.params)==2):
                        req_param=self.params[1]

                    res=self.post(self.p_port,self.params[0],{"fspath":req_param})
                    res=res.json()
                    if (res['code']!='0'):
                        print(res['error'])
                        continue
                    print(res['data'])

                elif(self.params[0]=='cat'):
                    if len(self.params)!=2:
                        print("enter command correctly")
                        continue
                    res=self.post(self.p_port,self.params[0],{"fspath":self.params[1]})
                    #expects response as file
                    self.getfileblocks(res)        

            except Exception as e:
                print(e)

def unload():
    subprocess.Popen(["python3", "-m", "src.unload_dfs"])

if __name__ == '__main__':
    try:
        if len(sys.argv) < 2:
            config_path = os.path.join(HADOOP_HOME, 'configs', 'dfs_setup_config.json')
            print("Using default config...")
        else:
            config_path = sys.argv[1]
        
        if not (os.path.exists(config_path)):
            print("path to config does not exist")
            exit(1)

        #get configs
        new_client = Client(config_path)
       
        #get ports
        new_client.startReqHandler()

    except KeyboardInterrupt:
        unload()
                        