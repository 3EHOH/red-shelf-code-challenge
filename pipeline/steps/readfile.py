import json
import os
from config import PathConfig


class ReadFile:

    def __init__(self):
        pass

    def readfile(self, pathname):
        path_to_file = os.path.join(PathConfig().target_path, pathname)

        with open(path_to_file, 'r') as f:
            file_output = json.load(f)

        return file_output
