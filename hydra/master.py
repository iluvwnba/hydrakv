import json
from json.decoder import JSONDecodeError
from typing import Dict
from socketserver import TCPServer, BaseRequestHandler


class HydraHandler(BaseRequestHandler):
    def handle(self) -> None:
        pass


class HydraMaster(TCPServer):
    def __init__(self, server_addr, rhc, data_path: str = 'hydra.json'):
        super().__init__(server_addr, rhc)
        self._path = data_path
        self.data: Dict = dict()
        self._slaves: Dict[(str, bool)] = dict()
        self.load()

    def handle(self) -> None:
        pass

    def load(self) -> None:
        with open(self._path, 'a+') as f:
            try:
                self.data = json.load(f)
            except JSONDecodeError:
                pass

    def dump(self) -> None:
        with open(self._path, 'w') as f:
            json.dump(self.data, f)

    def set(self, k: str, v):
        self.data[k] = v
        self.dump()
        self.send_wal_record_to_slaves(k, v)

    def delete(self, k: str):
        self.data.pop(k)
        self.dump()

    def get(self, k: str):
        return json.dumps(self.data[k])

    def get_all(self):
        return json.dumps(self._get_all_serialize())

    def _get_all_serialize(self):
        return {'keys': [k for k in self.data]}

    def register_slave(self, addr: str) -> (bool, str):
        # Adds to slaves but not connected
        if addr in self._slaves.keys():
            return False, "Slave already registered"
        self._slaves[addr] = False
        return True, "Registered Slave"

    def send_wal_record_to_slaves(self, k, v):
        pass


def run():
    import argparse
    parser = argparse.ArgumentParser(description="Run hydra master server")
    parser.add_argument('-host', dest='host', action='store')
    parser.add_argument('-port', dest='port', action='store')
    args = parser.parse_args()


if __name__ == "__main__":
    HOST, PORT = "localhost", 3000
    with HydraMaster((HOST, PORT), HydraHandler) as hydra:
        pass


class HydraConnector:
    def __init__(self):
        pass

    def get_read_connection(self):
        pass

    def get_write_connection(self):
        pass
