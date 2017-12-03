import json
import os
from config import PathConfig


class FileReader:

    def __init__(self):
        pass

    @staticmethod
    def read_file(pathname):
        path_to_file = os.path.join(PathConfig().target_path, pathname)

        with open(path_to_file, 'r') as f:
            file_output = json.load(f)

        return file_output
