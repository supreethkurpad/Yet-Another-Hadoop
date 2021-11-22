import hashlib

def hash(x):
    x = hashlib.sha256(x.encode())
    return x.hexdigest()
