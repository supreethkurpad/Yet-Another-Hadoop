import os
class DataNodeState:
    """
    Status:
    0 - Down
    1 - Up
    2 - Up but Full
    """
    def __init__(self, id, port, size_node, free_blocks, status):
        self.id = id
        self.port = port
        self.size_node = size_node
        self.free_blocks = free_blocks
        self.status = status