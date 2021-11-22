from flask import Flask, jsonify, request
from datetime import datetime 
import json
# from flask.logging import default_handler
import os
from sys import argv
import pickle
from state import DataNodeState
from random import choice
from threading import Thread
import time
from hash import hash
"""
Status codes:
0: [OK]
1: [Invalid fs path]
2: [File already Exists]
3: [Insufficient Memory in datanode]
"""


class NameNode :
    def __init__(self, port, dn_ports=[], path_to_config=None, primary=True):
        # init 
        self.server = Flask(__name__)
        self.port = port 
        self.primary = primary
        # self.server.logger.removeHandler(default_handler)
        self.datanodes = dn_ports
        
        print("HERE")
        # defines routes 
        self.initRequestHandler()

        # read config file
        with open(path_to_config, 'r') as f:
            self.config = json.load(f)

        if primary:
            self.path = self.config['path_to_primary']

        else:
            self.path = self.config['path_to_secondary']
        
        # gather state of datanodes
        self.datanode_states = self.readDataNodeStates()

        # start heartbeats
        # self.initHeartBeats()
        # start the server listening for requests
        self.server.run('127.0.0.1',port)

    def sendHeartBeats(self):
        response = requests.get('/')
        if response['message'] == 'Awake':
            self.datanode_states[response['id']].status = 1
        else:
            self.datanode_states[response['id']].status = 0
        time.sleep(5)
    
    def initHeartBeats(self):
        heartbeat = Thread(target=self.sendHeartBeats)
        # heartbeat.daemon = True
        heartbeat.start()
        
    def readDataNodeStates(self):
        """
        get all details about datanode such as size, available space, etc (status:0)
        """
        states = []
        datanode_path = self.config['path_to_datanodes']
        datanode_size = self.config['datanode_size']
        block_size = self.config['block_size']
        for i in range(len(self.datanodes)):
            data_dir = os.path.join(datanode_path, f"datanode{i+1}")
            free_blocks = datanode_size - sum(os.path.getsize(os.path.join(data_dir,f)) for f in os.listdir(data_dir))
            newState = DataNodeState(i+1, self.datanodes[i], block_size, free_blocks, status=1)
            states.append(newState)
        print(states)    
        return states

    
    def getBlocks(self, num_blocks, r):
        """
        Find r datanodes which are not full. 
        Pick an available index from each and return list of (id,index)
        Repeat process num_blocks times.
        """
        flag = False
        nodes = []
        for block in range(int(num_blocks)):
            available_dn = set(dn for dn in self.datanode_states if dn.status==1)
            for i in range(r):
                flag = True
                while(flag and len(available_dn)>0):
                    free_dn = choice(list(available_dn))
                    if free_dn.free_blocks>0:
                        nodes.append((free_dn.id, block))
                        available_dn.remove(free_dn)
                        flag = False
                    else:
                        available_dn.remove(free_dn)
                        free_dn.status=2

        if(len(nodes) == num_blocks*r):
            return nodes
        
        return -1

    def initRequestHandler(self):
        @self.server.route('/put',methods=['POST'])
        def put():
            """
            yah> -put filepath user/input/

            Req body:
            {
                filepath: path to file to be placed in the dfs
                path_in_fs: path in the dfs where file is to be placed
                size: size of file (no. of chunks)
            }

            Res body:
            {
                data: all metadata for the file
                code: 0 
                else
                error: error message
                code: error code
            }
            """
            req_data=request.json
            fspath = req_data.get("path_in_fs")
            print(fspath)
            num_blocks = int(req_data.get('size'))
            filepath = req_data.get('filepath')
            filename = os.path.basename(filepath)
            

            if not fspath.startswith(self.config['fs_path']):
                fspath = os.path.join(self.config["fs_path"], fspath)
            
            print(fspath)

            actual_path = os.path.join(self.path, fspath, filename)

            if not os.path.exists(os.path.dirname(actual_path)):
                return {"error":"FSPath Not Found", "code":"1"}

            print(actual_path)
            if os.path.exists(actual_path):
                return {"error":"file with same name already exists", "code":"2"}
            
            newfile = open(actual_path, 'w')

            blocks = self.getBlocks(num_blocks, int(self.config['replication_factor']))
            if blocks == -1:
                return {"error":"Not enough memory!", "code":"3"}

            count = 1    
            for d_id, block_idx in blocks:
                print(d_id, hash(fspath+str(block_idx)), file=newfile, sep=",", end=" ")
                
                if count%int(self.config['replication_factor'])==0:
                    print(file=newfile)
                
                count += 1
            newfile.close()

            with open(actual_path, 'r') as f:
                return {
                    "data":f.read(),
                    "code":"0"
                }

        @self.server.route('/cat',methods=['POST'])
        def cat():
            """
            yah> cat fspath
            """
            req_data=request.json
            fspath = req_data.get('fspath')
            fspath = os.path.join(self.path, fspath)
            if not os.path.exists(fspath):
                return {
                    "error":"fspath not found",
                    "code":"1"
                }
            
            with open(fspath, 'r') as f:
                metadata = f.read()
            
            return {
                "data": metadata,
                "code": "0"
            }


        @self.server.route('/rmdir',methods=['POST'])
        def rmdir():
            req_data=request.json
            #req_data={"dpath":"path/to/file"}
            return "dir deleted successfully"
            pass

        @self.server.route('/mkdir',methods=['POST'])
        def mkdir():
            req_data=request.json
            #req_data={"dpath":"path/to/dir"}
            return "Folder created successfully"
            pass
        
        @self.server.route('/ls',methods=['POST'])
        def ls():
            req_data=request.json
            #req_data={"dpath":"path/to/dir"}
            #return ['/test.txt','test2.txt'] list of files under dir
            return[]
        
        @self.server.route('/rm',methods=['POST'])
        def rm():
            req_data=request.json
            #req_data={"path":"path/to/file"}
            return "File/folder deleted successfully"
            pass
        
        @self.server.route('/')
        def heartbeat():
            if self.primary:
                return jsonify(port=self.port, timestamp=datetime.now())
            else:
                # TODO
                pass


if __name__ == "__main__":
    argpath = argv[1]
    with open(argpath, 'rb') as f:
        args = pickle.load(f)
    NameNode(*args)
