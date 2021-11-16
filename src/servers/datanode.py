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
    
    #dn -> self.file -> data
    #read -> read from an index in self.file
    #write -> write at an index in self.file
    def initRequestHandler(self):
        
        @self.server.route('/write', methods=['POST'])
        def write(self, index, data):
            index = request.get("index")
            data = request.get("data")
            index*=self.block_size

            with open("index.dat", "ab+") as f:
                f.seek(index, 0)
                f.write(data)

            return jsonify(id = self.id, index = index)

        @self.server.route('/read/<index>')
        def read(self, index):
            #return with data
            index = request.get(index)
            index*=self.block_size
            with open(self.file, "rb") as f:
                f.seek(index, 0)
                data = f.read(self.block_size)
                return jsonify(id = self.id, index = index, data=data)
            
        
        @self.server.route('/')
        def heartbeat(self):
            return jsonify(id = self.id, message="Awake")


if __name__ == '__main__':
    argpath = argv[1]
    with open(argpath, 'rb') as f:
        args = pickle.load(f)
    datanode = DataNode(*args)
    pass