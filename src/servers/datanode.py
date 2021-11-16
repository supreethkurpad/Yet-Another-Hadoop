from flask import Flask, request, jsonify
from datetime import datetime 
from flask.logging import default_handler
from sys import argv
import pickle
import os
import json

class DataNode :
    def __init__(self, id, port, block_size):
        # init 
        self.server = Flask(__name__)
        self.port = port 
        self.block_size = block_size
        self.id = id
        self.HADOOP_HOME = os.environ['MYHADOOP_HOME']
        # defines routes 
        self.readConfig()
        self.data_dir = os.path.join(self.config['path_to_datanodes'], f"datanode_{self.id}")

        self.initRequestHandler()

        # start the server listening for requests
        self.server.run(port = port)
    
    def readConfig(self):
        with open(os.path.join(self.HADOOP_HOME, 'configs', 'dfs_setup_config.json'), 'w') as f:
            self.config = json.load(f) 

    def initRequestHandler(self):
        
        @self.server.route('/write', methods=['POST'])
        def write():
            rs = request.json
            index = rs.get("index")
            data = rs.get("data").encode('utf-8')


            with open(f"{index}.bin", "ab+") as f:
                f.write(data)

            return jsonify(id = self.id, index = index)

        @self.server.route('/read/<index>')
        def read(index):
            #return with data
            with open("{index}.bin", "rb") as f:
                data = f.read(index)
                return jsonify(id = self.id, index = index, data=data)
            
        
        @self.server.route('/')
        def heartbeat():
            return jsonify(id = self.id, message="Awake")


if __name__ == '__main__':
    args = argv[1:]
    print("DataNode ", args)
    try:
        datanode = DataNode(int(args[0]), int(args[1]), int(args[2]))    
    except Exception as e:
        print(f"Datanode {args[0]} crashed on port {args[1]}", e)