import os
import requests

def partition(filepath,chunk_size):
    chunks=[]
    if not os.path.exists(filepath):
        print("File does not exist in path specified")
        return chunks
    with open(filepath, 'rb') as f:
        chunk = f.read(chunk_size)
        while chunk:
            chunks.append(chunk)
            chunk = f.read(chunk_size)
    return chunks


def post(port,params):
    try:
        res=request.post('http://localhost:'+str(port)+'/put',data=json.dumps(params)) 
        return res
    except:
            print("no datanode found in port")
    

if __name__ == '__main__':
    
    config_path=input()
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

    while True:
        
        req=input()
        #put file /dir 
        parameters=req.split(" ")

        try:
            if(parameters[0]=='put'):
                if len(parameters)!=3:
                    print("Incorrect number of parameters, enter file path and hdfs dir")
                    continue

                chunks=partition(parameter[1],config['datanode_size']) 
                if(len(chunks)==0):
                        continue

                for i in len(chunks):
                    #request namenode
                    res=post(5000,{"fpath":parameter[1]}) #see of this json way of passing param is needed
                    
                    #recieves dict of datanode:index
                    final_res=post(ports[res[0]-1],{"data":chunk,"nodes":res,"rep_cnt":config['replication_factor'],\
                        "ports":ports})
            #elif(parameters[0]=='ls'):

        except Exception as e:
            print(e)
            

            
        #if()
            