import socket
from contextlib import closing


def getPortNumbers(n, blacklist=[]) -> list:
    ports = []
    while n:
        with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as a_socket:
            a_socket.bind(('', 0))
            a_socket.getsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            port = a_socket.getsockname()[1]
            if port not in blacklist and port not in ports:
                ports.append(port)
            else:
                continue
            n-=1
        
    return ports   