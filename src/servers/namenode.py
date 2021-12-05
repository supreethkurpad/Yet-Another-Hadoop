from flask import Flask, jsonify, request
from datetime import datetime 
import json
import os
from sys import argv
import pickle
from random import choice
from threading import Thread
import time
import requests
import subprocess
from pathlib import Path
from shutil import copytree

from src.servers.edit_log import LogWriter, LogReader, Operation
from src.utils.state import DataNodeState
from src.utils.hash import hash
from src.utils.port_finder import getPortNumbers


"""
Status codes for yah operations:
0: [OK]
1: [Invalid fs path]
2: [File already Exists]
3: [Insufficient Memory in datanode]
"""

HADOOP_HOME = os.environ['MYHADOOP_HOME']
LOG_FILE='edits.txt'
class NameNode :
    def __init__(self, port, dn_ports=[], path_to_config=None, primary=True, _name='namenode_1'):

        self.server = Flask(_name)
        self.port = port 
        self.primary = primary
        self.datanodes = dn_ports
        self._name = _name
        self.path_to_config = path_to_config

        # defines routes 
        self.initRequestHandler()

        # read config file
        with open(path_to_config, 'r') as f:
            self.config = json.load(f)

        if primary:
            self.path = self.config['path_to_primary']
            self.edits = []
            self.snn_port = self.config['snn_port']

        else:
            self.path = self.config['path_to_secondary']
        
        self.log_file = os.path.join(self.config['namenode_log_path'], LOG_FILE)
        with open(self.log_file, 'w') as f:
            f.write("")

        # creates a log writer/reader
        if self.primary:
            self.logWriter = LogWriter(self.log_file)
        else:
            self.logReader = LogReader(self.path)

        # gather state of datanodes
        self.datanode_states = self.readDataNodeStates()
            
        # start heartbeats
        self.initHeartBeats()

        # start the server listening for requests
        self.server.run('127.0.0.1',port)

    def switchToPrimary(self):
        """
        Marks the calling instance of namenode as the primary namenode
        """
        self.primary = True
        self.initHeartBeats()
        self.logWriter = LogWriter(self.log_file)
        del self.logReader
        self.edits = []
        print("Failover Mechanism Successfull. Some data may have been lost as edit logs are not instantaneous")

    def sendHeartBeats(self):
        """
        Send heartbeat requests to the datanodes to check if they are alive
        and update status accordingly.
        """
        while self.primary:
            for i in range(len(self.datanodes)):
                port = self.datanodes[i]
                try:
                    response = requests.get(f'http://localhost:{port}/').json()
                    if response['message'] == 'Awake':
                        self.datanode_states[response['id']-1].status = 1
                    else:
                        self.datanode_states[response['id']-1].status = 0

                except Exception as e:
                    self.datanode_states[i].status = 0
                    continue
            # write edits to log file
            self.logWriter.write_logs(self.edits)
            self.edits = []
            time.sleep(self.config['sync_period'])
    
    def snnHeartbeat(self):
        """
        If self is a secondary namenode, sends heartbeats to the primary namenode 
        and in case of failure, take over as the new primary and starts a new secondary  
        """
        while not self.primary:
            time.sleep(self.config['sync_period'])
            pnn_port = self.config['pnn_port']
            try:
                res = requests.get(f'http://localhost:{pnn_port}/').json()
                if res['message'] == 'Awake':
                    log_file = res['log_file']
                    self.logReader.read_log(log_file)
            except Exception as e:
                self.initiateFailover()

    def initiateFailover(self):
        print("Namenode Down! Trying to recover data and moving to Secondary Namenode")
        s_namenode = os.path.join(HADOOP_HOME, 'src' , 'servers', 'namenode.py')
        snn_port = getPortNumbers(1, blacklist=self.datanodes+[self.port])[0]
        self.snn_port = snn_port
        s_namenode_args = [snn_port, self.datanodes, self.path_to_config, False, f'namenode_{int(self._name.split("_")[-1])+1}']
        s_argpath = os.path.join(HADOOP_HOME, 'tmp', 's_namenode_arg.pickle')
        with open(s_argpath, 'wb') as f:
            pickle.dump(s_namenode_args, file=f)

        self.config['path_to_s_argpath']=s_argpath
        self.config['path_to_primary'] = self.path
        self.config['path_to_secondary'] = os.path.join(self.config['path_to_namenodes'], s_namenode_args[4].replace(" ", "").lower())

        src = self.config['path_to_primary']
        dest = self.config['path_to_secondary']
        print(src, dest)
        copytree(src, dest)

        new_nn = subprocess.Popen(['python3', "-m", "src.servers.namenode", s_argpath])

        with open(os.path.join(HADOOP_HOME, 'tmp', 'pids.txt'), 'a') as f:
           print(new_nn.pid, file=f)
        
        self.config['pnn_port'] = self.port
        self.config['snn_port'] = self.snn_port
        with open(self.path_to_config, 'w') as f:
            json.dump(self.config, f)
        
        self.switchToPrimary()

    def initHeartBeats(self):
        if self.primary:
            self.heartbeat = Thread(target=self.sendHeartBeats)
            self.heartbeat.start()

        else:
            self.snn_heartbeat = Thread(target=self.snnHeartbeat)
            self.snn_heartbeat.start()

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
            free_blocks = free_blocks // block_size
            newState = DataNodeState(i+1, self.datanodes[i], datanode_size, free_blocks, status=1)
            states.append(newState)
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
            for node in nodes:
                self.datanode_states[node[0]-1].free_blocks -= 1
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
            num_blocks = int(req_data.get('size'))
            filepath = req_data.get('filepath')
            filename = os.path.basename(filepath)
            
            fspath = os.path.join(fspath, filename)

            if not fspath.startswith(self.config['fs_path']):
                fspath = os.path.join(self.config["fs_path"], fspath)
            

            actual_path = os.path.join(self.path, fspath)
            if not os.path.exists(os.path.dirname(actual_path)):
                return json.dumps({"error":"FSPath Not Found", "code":"1"})

            if os.path.exists(actual_path):
                return json.dumps({"error":"file with same name already exists", "code":"2"})
            

            blocks = self.getBlocks(num_blocks, int(self.config['replication_factor']))
            if blocks == -1:
                return json.dumps({"error":"Not enough memory!", "code":"3"})

            newfile = open(actual_path, 'w')

            count = 1    
            for d_id, block_idx in blocks:
                print(d_id, hash(fspath+str(block_idx)), file=newfile, sep=",", end=" ")
                
                if count%int(self.config['replication_factor'])==0:
                    print(file=newfile)
                
                count += 1
            newfile.close()

            with open(actual_path, 'r') as f:
                data = f.read()

            if self.primary:
                self.edits.append(Operation('put', fspath, data))

            return json.dumps({
                "data":data,
                "code":"0"
            })

        @self.server.route('/cat',methods=['POST'])
        def cat():
            """
            yah> cat fspath
            """
            req_data=request.json
            fspath = req_data.get('fspath')
            if not fspath.startswith(self.config['fs_path']):
                fspath = os.path.join(self.config['fs_path'], fspath)

            fspath = os.path.join(self.path, fspath)
            if not os.path.exists(fspath):
                return json.dumps({
                    "error":"fspath not found",
                    "code":"1"
                })
            
            with open(fspath, 'r') as f:
                metadata = f.read()
            
            return json.dumps({
                "data": metadata,
                "code": "0"
            })


        @self.server.route('/rmdir',methods=['POST'])
        def rmdir():
            """
            yah> rmdir fspath
            """
            req_data=request.json
            fspath = req_data.get('fspath')
            if not fspath.startswith(self.config['fs_path']):
                fspath = os.path.join(self.config['fs_path'], fspath)

            try:
                acutal_path = os.path.join(self.path, fspath)
                os.rmdir(acutal_path)
            except:
                return json.dumps({
                    "error":"Could not delete directory. Check the path and also make sure it is empty",
                    "code":"1"
                })

            if self.primary:
                self.edits.append(Operation('rmdir', fspath))

            return json.dumps({
                "code":"0",
                "msg":"deleted directory"
            })

        @self.server.route('/mkdir',methods=['POST'])
        def mkdir():
            """
            yah> mkdir fspath
            """
            req_data=request.json
            fspath = req_data.get('fspath')
            if not fspath.startswith(self.config['fs_path']):
                fspath = os.path.join(self.config['fs_path'], fspath)
            
            if os.path.exists(fspath):
                return json.dumps({
                    "error":"directory already exists",
                    "code":"1"
                })

            try:
                acutal_path = os.path.join(self.path, fspath)
                os.mkdir(acutal_path)
            except:
                return json.dumps({
                    "error":"Invalid path to directory",
                    "code":"1"
                })

            if self.primary:
                self.edits.append(Operation('mkdir', fspath))

            return json.dumps({
                "code":"0",
                "msg":"created directory"
            })
        
        @self.server.route('/ls',methods=['POST'])
        def ls():
            """
            req{
                fspath:
            }
            res{
                error:"msg"
                code: not 0

                else

                data:"string output of ls"
                code:0
            }
            """
            req_data=request.json
            fspath = req_data.get('fspath')

            if not fspath.startswith(Path(self.config['fs_path']).stem):
                fspath = os.path.join(self.config['fs_path'], fspath)

            actual_path = os.path.join(self.path, fspath)
            if not os.path.exists(actual_path):
                return json.dumps({ 
                    "code":"1",
                    "error":"No such File/Directory"
                })
            if os.path.isfile(actual_path):
                return json.dumps({
                    "data": os.path.basename(actual_path)
                })
            return json.dumps({
                "data": '\n'.join(os.listdir(actual_path)),
                "code": "0"
            })
        
        @self.server.route('/rm',methods=['POST'])
        def rm():
            """
            req{
                fspath
            }
            res{
                data: all metadata of the file
            }
            """
            req_data=request.json
            fspath = req_data.get('fspath')
            if not fspath.startswith(self.config['fs_path']):
                fspath = os.path.join(self.config['fs_path'], fspath)
            actual_path = os.path.join(self.path, fspath)

            if not os.path.exists(actual_path):
                return json.dumps({
                    "error":"file not found",
                    "code":"1"
                })
            with open(actual_path, 'r') as f:
                metadata = f.read()
            
            for line in metadata.strip().split('\n'):
                for tup in line.strip().split(' '):
                    id, _ = tup.split(',')
                    self.datanode_states[int(id)-1].free_blocks += 1

            if self.primary:
                self.edits.append(Operation('rm', fspath))

            os.remove(actual_path)
            return json.dumps({
                "data":metadata,
                "code":"0"
            })
        
        @self.server.route('/')
        def heartbeat():
            if self.primary:
                return jsonify(message="Awake",
                 port=self.port,
                 timestamp=datetime.now(),
                 log_file=self.log_file)
            else:
                return jsonify(message="Awake")


if __name__ == "__main__":
    argpath = argv[1]
    with open(argpath, 'rb') as f:
        args = pickle.load(f)
    NameNode(*args)
