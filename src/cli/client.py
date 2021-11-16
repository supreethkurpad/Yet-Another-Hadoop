import os
import requests
import sys
import json
HADOOP_HOME=os.environ.get('MYHADOOP_HOME','/home/swarupa/College/Sem5/Yet-Another-Hadoop/')


class Client:
    def __init__(self,config,ports):
        self.config=config
        self.ports=ports
        self.params=None
        self.chunks=None

    def post(self,port,params):
        try:
            res=requests.post('http://localhost:'+str(port)+'/put',data=json.dumps(params)) 
            return res
        except:
                print("no datanode found in port")

    def partition(self):
        if(self.params is None):
            print("Enter file path")
            return
        print(self.params[1])
        if not os.path.exists(self.params[1]):
            print("File does not exist in path specified")
            return

        with open(self.params[1], 'rb') as f:
            chunk = f.read(self.config['block_size'])
            while chunk:
                self.chunks=chunk
                self.sendRequest()
                chunk = f.read(self.config['block_size'])

    def sendRequest(self):
        res=None
        res=self.post(5000,{"fpath":self.params[1]}) #see of this json way of passing param is needed
                        
        if(res==None):
            print("no ports running, try again")
            return
        #recieves dict of datanode:index
        final_res=self.post(self.ports[res[0]-1],{"data":self.chunks,"nodes":res,"rep_cnt":self.config['replication_factor'],\
                            "ports":self.ports})



    

    def startReqHandler(self):
        while True:
        
            req=input("yah>")
            #put file /dir 
            self.params=req.split(" ")

            try:
                if(self.params[0]=='put'):
                    if len(self.params)!=3:
                        print("Incorrect number of parameters, enter file path and hdfs dir")
                        continue
                    self.partition()                         
                #elif(parameters[0]=='ls'):

            except Exception as e:
                print(e)



        

if __name__ == '__main__':

    if len(sys.argv) < 2:
        config_path = os.path.join(HADOOP_HOME, 'configs', 'dfs_setup_config.json')
        print("Using default config...")
    else:
        config_path = sys.argv[1]
    
    if not (os.path.exists(config_path)):
        print("path to config does not exist")
        exit(1)

    #get configs
    config = {}
    with open(config_path, 'r') as f:
        config = json.load(f)

    #get ports
    with open(config['path_to_ports'],'r') as f:
        ports=f.read().splitlines()

    new_client = Client(config,ports)
    new_client.startReqHandler()
                