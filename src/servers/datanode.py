from flask import Flask, request, jsonify
from datetime import datetime 
from flask.logging import default_handler
from sys import argv
import pickle

class DataNode :
    def __init__(self, id, port, block_size):
        # init 
        self.server = Flask(__name__)
        self.port = port 
        self.block_size = block_size
        self.id = id
        # defines routes 
        self.initRequestHandler()

        # start the server listening for requests
        self.server.run('127.0.0.1',port)
    
    #dn -> {index}.bin -> data
    #read -> read from an index in {index}.bin
    #write -> write at an index in {index}.bin
    def initRequestHandler(self):
        
        @self.server.route('/write', methods=['POST'])
        def write(self, index, data):
            index = request.get("index")
            data = request.get("data")
            index*=self.block_size

            with open(f"{index}.bin", "ab+") as f:
                f.seek(index, 0)
                f.write(data)

            return jsonify(id = self.id, index = index)

        @self.server.route('/read/<index>')
        def read(self, index):
            #return with data
            index = request.get(index)
            index*=self.block_size
            with open("{index}.bin", "rb") as f:
                f.seek(index, 0)
                data = f.read(self.block_size)
                return jsonify(id = self.id, index = index, data=data)
            
        
        @self.server.route('/')
        def heartbeat(self):
            return jsonify(id = self.id, message="Awake")


if __name__ == '__main__':
    argpath = argv[1]
    args = argpath.split(" ")
    print("DataNode ", args)
    try:
        datanode = DataNode(int(args[0]), int(args[1]), int(args[2]))    
    except Exception as e:
        print(f"Datanode {args[0]} crashed on port {args[1]}")