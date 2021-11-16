from flask import Flask, jsonify
from datetime import datetime 
import json
from flask.logging import default_handler
import os
from sys import argv
import pickle

class NameNode :
    def __init__(self, port, dn_ports=[], path_to_config=None, primary=True):
        # init 
        self.server = Flask(__name__)
        self.port = port 
        self.primary = primary
        self.server.logger.removeHandler(default_handler)
        self.datanodes = dn_ports
        print("NameNode ", port)
        # defines routes 
        self.initRequestHandler()

        # store received heartbeats (might not be needed)
        self.heartbeats_rcvd = []

        # read config file
        with open(path_to_config, 'r') as f:
            self.config = json.load(f)

        # start the server listening for requests
        self.server.run('127.0.0.1',port)
        
        pass

    def heartbeatHandler(self, beat):
        pass

    def initRequestHandler(self):
        @self.server.route('/put/<file>/<path>')
        def put(file, path_in_fs):
            """
            client -put myfile.txt user/input/
            """
            pass

        @self.server.route('/cat/<path>')
        def cat(path_in_fs):
            pass

        @self.server.route('/rmdir/<path>')
        def rmdir(path_in_fs):
            pass

        @self.server.route('/mkdir/<path>')
        def mkdir(path_in_fs):
            pass
        
        @self.server.route('/ls/<path>')
        def ls(path_in_fs):
            pass
        
        @self.server.route('/rm/<path>')
        def rm(path_in_fs):
            pass
        
        @self.server.route('/')
        def heartbeat():
            if self.primary:
                return jsonify(port=self.port, timestamp=datetime.now())
            else:
                # TODO
                pass

    def checkPathExists(self, path_in_fs):
        pass

if __name__ == "__main__":
    # TODO unpickle args and pass them
    argpath = argv[1]
    with open(argpath, 'rb') as f:
        args = pickle.load(f)
    namenode = NameNode(*args)