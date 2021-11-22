import os



class DataNodeState:
    def __init__(self, port, size_node, free_blocks, status):
        self.port = port
        self.size_node = size_node
        self.free_blocks = free_blocks
        self.status = status
    