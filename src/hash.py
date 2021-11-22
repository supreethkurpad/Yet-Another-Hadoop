import hashlib

def hash(x):
    x = hashlib.sha512(x.encode())
    return x.hexdigest()
