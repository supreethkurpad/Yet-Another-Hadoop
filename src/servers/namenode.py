from flask import Flask

class NameNode :
    def __init__(self, port, path_to_config):
        self.server = Flask(__name__)
        self.path_to_config = path_to_config
        self.port = port 

    def requestHandler(self):
        self.server.route()
    