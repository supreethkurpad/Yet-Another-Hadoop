from flask import Flask, jsonify, request
from datetime import datetime 
import json
from flask.logging import default_handler
import os
from sys import argv
import pickle
from flask_apscheduler import APScheduler
from state import DataNodeState
from random import choice, sample

"""
Status codes:
0: [OK]
1: [Invalid fs path]
2: [File already Exists]
3: [Insufficient Memory in datanode]
"""
class Config(object):
    JOBS = [
        {
            'id': 'job1',
            'func': 'namenode:initHeartBeats',
            'args': (),
            'trigger': 'interval',
            'seconds': 10
        }
    ]

class NameNode :
    def __init__(self, port, dn_ports=[], path_to_config=None, primary=True):
        # init 
        self.server = Flask(__name__)
        self.port = port 
        self.primary = primary
        self.server.logger.removeHandler(default_handler)
        self.datanodes = dn_ports
        self.server.config.from_object(Config())
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
        scheduler = APScheduler()
        scheduler.init_app(self.server)
        scheduler.start()
        # start the server listening for requests
        self.server.run('127.0.0.1',port)

        

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
            newState = DataNodeState(i+1, self.datanodes[i], block_size, free_blocks, status=0)
            states.append(newState)
        print(states)    
        return states

    def initHeartBeats(self):
        print(datetime.now())
    
    def getBlocks(self, num_blocks, r):
        """
        Find r datanodes which are not full. 
        Pick an available index from each and return list of (id,index)
        Repeat process num_blocks times.
        """
        available_dn = self.datanode_states
        for block in range(num_blocks):
            for i in range(r):
                free_dn = choice(available_dn)

        pass

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
            num_blocks = req_data.get('size')
            filepath = req_data.get('filepath')

            if not fspath.startswith(self.config['fs_path']):
                fspath = os.path.join(self.config["fs_path"], fspath)
            
            fspath = os.path.join(self.path, fspath)
            
            if not os.path.exists(os.path.dirname(fspath)):
                return {"error":"FSPath Not Found", "code":"1"}

            if os.path.exists(fspath):
                return {"error":"file with same name already exists", "code":"2"}
            
            newfile = open(fspath, 'w')

            blocks = self.getBlocks(num_blocks, int(self.config['replication_factor']))
            if not blocks:
                return {"error":"Not enough memory!", "code":"3"}

            for block in blocks:
                for d_id, idx in block:
                    print(d_id, idx, file=newfile, sep=",", end=" ")
                print(file=newfile)
            newfile.close()

            with open(fspath, 'r') as f:
                return {
                    "data":fspath.read(),
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
    namenode = NameNode(*args)from flask import Flask, jsonify, request
from datetime import datetime 
import json
from flask.logging import default_handler
import os
from sys import argv
import pickle
from flask_apscheduler import APScheduler
from state import DataNodeState
from random import choice, sample

"""
Status codes:
0: [OK]
1: [Invalid fs path]
2: [File already Exists]
3: [Insufficient Memory in datanode]
"""
class Config(object):
    JOBS = [
        {
            'id': 'job1',
            'func': 'namenode:initHeartBeats',
            'args': (),
            'trigger': 'interval',
            'seconds': 10
        }
    ]

class NameNode :
    def __init__(self, port, dn_ports=[], path_to_config=None, primary=True):
        # init 
        self.server = Flask(__name__)
        self.port = port 
        self.primary = primary
        self.server.logger.removeHandler(default_handler)
        self.datanodes = dn_ports
        self.server.config.from_object(Config())
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
        scheduler = APScheduler()
        scheduler.init_app(self.server)
        scheduler.start()
        # start the server listening for requests
        self.server.run('127.0.0.1',port)

        

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
            newState = DataNodeState(i+1, self.datanodes[i], block_size, free_blocks, status=0)
            states.append(newState)
        print(states)    
        return states

    def initHeartBeats(self):
        print(datetime.now())
    
    def getBlocks(self, num_blocks, r):
        """
        Find r datanodes which are not full. 
        Pick an available index from each and return list of (id,index)
        Repeat process num_blocks times.
        """
        available_dn = self.datanode_states
        for block in range(num_blocks):
            for i in range(r):
                free_dn = choice(available_dn)

        pass

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
            num_blocks = req_data.get('size')
            filepath = req_data.get('filepath')

            if not fspath.startswith(self.config['fs_path']):
                fspath = os.path.join(self.config["fs_path"], fspath)
            
            fspath = os.path.join(self.path, fspath)
            
            if not os.path.exists(os.path.dirname(fspath)):
                return {"error":"FSPath Not Found", "code":"1"}

            if os.path.exists(fspath):
                return {"error":"file with same name already exists", "code":"2"}
            
            newfile = open(fspath, 'w')

            blocks = self.getBlocks(num_blocks, int(self.config['replication_factor']))
            if not blocks:
                return {"error":"Not enough memory!", "code":"3"}

            for block in blocks:
                for d_id, idx in block:
                    print(d_id, idx, file=newfile, sep=",", end=" ")
                print(file=newfile)
            newfile.close()

            with open(fspath, 'r') as f:
                return {
                    "data":fspath.read(),
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
    namenode = NameNode(*args)