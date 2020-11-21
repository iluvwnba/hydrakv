import json
from json.decoder import JSONDecodeError
from typing import Dict
from socketserver import BaseRequestHandler, TCPServer


class HydraMaster(BaseRequestHandler):
    def __init__(self, host: str, port: int, data_path: str = 'hydra.json'):
        self._path = data_path
        self.data: Dict = dict()
        self.slaves: Dict[(str, bool)] = dict()

    def handle(self) -> None:
        print(self.client_address)
        while True:
            msg = self.request.recv(8192)
            if not msg:
                break
            self.request.send(msg)

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
        return json.dumps(self.data[k])

    def get_all(self):
        return json.dumps(self._get_all_serialize())

    def _get_all_serialize(self):
        return {'keys': [k for k in self.data]}

    def register_slave(self, addr: str):
        # Adds to slaves but not connected
        self.slaves[addr] = False
        self.create_slave_connection()

    def create_slave_connection(self):
        pass


def run():
    import argparse
    parser = argparse.ArgumentParser(description="Run hydra master server")
    parser.add_argument('-host', dest='host', action='store')
    parser.add_argument('-port', dest='port', action='store')
    args = parser.parse_args()

    serv = TCPServer(('', 20000), HydraMaster)
    serv.serve_forever()


if __name__ == "__main__":
    run()


class HydraConnector:
    def __init__(self):
        pass

    def get_read_connection(self):
        pass

    def get_write_connection(self):
        pass
