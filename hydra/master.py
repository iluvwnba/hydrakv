import json
from json.decoder import JSONDecodeError
from typing import Dict
from socketserver import TCPServer, BaseRequestHandler
import socket
from xmlrpc.server import SimpleXMLRPCServer


class HydraHandler(BaseRequestHandler):
    def handle(self) -> None:
        # noinspection PyTypeChecker
        h: HydraMaster = self.server
        h.register_slave(self.client_address)


class HydraMaster(TCPServer):
    _rpc_methods_ = ['set', 'delete']

    def __init__(self, server_addr, rhc, data_path: str = 'hydra.json'):
        super().__init__(server_addr, rhc)
        self._path = data_path
        self.data: Dict = dict()
        self._slaves: Dict[(str, bool)] = dict()
        self.load()
        self._addr = server_addr

        self._serv = SimpleXMLRPCServer(('localhost', 3110), allow_none=True)
        for name in self._rpc_methods_:
            self._serv.register_function(getattr(self, name))
        self._serv.serve_forever()

    def load(self) -> None:
        with open(self._path, 'r') as f:
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
        self._slaves[addr] = True
        return True, "Registered Slave"

    def send_wal_record_to_slaves(self, operation, k, v):
        test = {
            "id": 1,
            "operation": "type",
            "key": "<KEY>",
            "value": "<VALUE>"
        }
        for slave, alive in self._slaves.items():
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.bind(self._addr)
                if alive:
                    sock.connect(slave)
                    sock.sendall(bytes(json.dumps(test)))
                    recv = sock.recv(1024).decode("utf-8")
                    print(recv)


def run():
    import argparse
    parser = argparse.ArgumentParser(description="Run hydra master server")
    parser.add_argument('-host', dest='host', action='store')
    parser.add_argument('-port', dest='port', action='store')
    args = parser.parse_args()


if __name__ == "__main__":
    HOST, PORT = "localhost", 3000
    with HydraMaster((HOST, PORT), HydraHandler) as hydra:
        hydra: HydraMaster
        hydra.serve_forever()

