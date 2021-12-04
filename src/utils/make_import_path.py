import os
from pathlib import Path

def make_import_path(filepath):
    return Path(filepath.replace('/', '.').replace('\\','.')).stem
    