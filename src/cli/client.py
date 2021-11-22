import os
import requests
import sys
import json
import urllib.request

HADOOP_HOME=os.environ.get('MYHADOOP_HOME','/home/swarupa/College/Sem5/Yet-Another-Hadoop/')


class Client:
    def __init__(self,config,ports):
        self.config=config
        self.ports=ports
        self.params=None
        self.chunks=None

    def post(self,port,cmd,data):
        if((requests.head('http://localhost:'+str(port))).status_code==200):
            try:
                res=requests.post('http://localhost:'+str(port)+'/'+str(cmd),data=json.dumps(data)) 
                return res
            except:
                    print("no node found in port")
        return None

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
                self.sendputRequest()
                chunk = f.read(self.config['block_size'])

    def sendputRequest(self):
        res=None
        res=self.post(5000,self.params[0],{"fpath":self.params[1]}) #see of this json way of passing param is needed
                        
        if(res.data()==None):
            print("no ports running, try again")
            return
        print("got from namenode")
        #recieves dict of datanode:index
        final_res=self.post(self.ports[res[0]-1],'write',{"data":self.chunks,"nodes":res,"rep_cnt":self.config['replication_factor'],\
                            "ports":self.ports})
        if(final_res.data()==None):
            print("failed to insert file")

    #res={1:[2,3],23:[1,2]}
    def getfileblocks(self,res):
        for i in res:
            for p in res[i]:
                data=self.post(self.ports[i[p-1]],'read',{"file_index":i})
                if(data is None):
                    print("datanode down")
                else:
                    print(int(data,2))
                    break
            

    

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

                elif(self.params[0]=='mkdir'):
                    if len(self.params)!=2:
                        print('Incorrect number of parameters, enter name of one directory')
                        continue
                    res=self.post(5000, self.params[0], {'dpath':self.params[1]})
                    print(res.text) 
                
                # Delete file or directory
                elif(self.params[0]=='rm'): 
                    print(self.params[0])
                    if len(self.params)!=2:
                        print('Incorrect number of parameters, enter name file or directory')
                        continue
                    res=self.post(5000, self.params[0], {'path':self.params[1]})
                    print(res.text) 
                
                elif(self.params[0]=='rmdir'):
                    if len(self.params)!=2:
                        print('Incorrect number of parameters, enter name of one directory')
                        continue
                    res=self.post(5000, self.params[0], {'dpath':self.params[1]})
                    print(res.text) 

                elif(self.params[0]=='ls'):
                    if len(self.params)!=2:
                        print("enter command correctly")
                        continue
                    res=None
                    res=self.post(5000,self.params[0],{"fpath":self.params[1]})
                    if (res== None):
                        print("Directory doesnt exist in hdfs")
                        return
                    print("files are:")
                    for i in res:
                        print(res)
                elif(self.params[0]=='cat'):
                    if len(self.params)!=2:
                        print("enter command correctly")
                        continue
                    res=self.post(5000,self.params[0],{"fpath":self.params[1]})
                    if(res==None):
                        print("file not found in hdfs")
                    #expects response as {fileblock:datanode}
                    self.getfileblocks(res)

                    
                    

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
                