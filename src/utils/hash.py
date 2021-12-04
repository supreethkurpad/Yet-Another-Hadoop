import hashlib

def hash(x):
    x = hashlib.md5(x.encode())
    return x.hexdigest()
