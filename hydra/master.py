import json
from json.decoder import JSONDecodeError
from typing import Dict


class HydraMaster:
    def __init__(self, data_path: str = 'hydra.json'):
        self._path = data_path
        self.data: Dict = dict()

    def load(self):
        with open(self._path, 'a+') as f:
            try:
                self.data = json.load(f)
            except JSONDecodeError:
                pass

    def dump(self):
        with open(self._path, 'w') as f:
            json.dump(self.data, f)

    def set(self, k: str, v):
        self.data[k] = v
        self.dump()

    def delete(self, k: str):
        self.data.pop(k)
        self.dump()

    def get(self, k: str):
        return self.data[k]
