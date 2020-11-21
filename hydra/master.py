import json
from json.decoder import JSONDecodeError
from typing import Dict
from flask import Flask, request
import urllib.request


class HydraMaster:
    def __init__(self, host: str, port: int, data_path: str = 'hydra.json'):
        self._path = data_path
        self.data: Dict = dict()
        self._slaves: Dict[(str, bool)] = dict()
        self.load()

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
        for slave in self._slaves:
            req = urllib.request.Request("http://{}/hydra/api/wal".format(slave))
            req.add_header('Content-Type', 'application/json; charset=utf-8')
            jsondata = json.dumps({k: v})
            jsondataasbytes = jsondata.encode('utf-8')  # needs to be bytes
            req.add_header('Content-Length', len(jsondataasbytes))
            response = urllib.request.urlopen(req, jsondataasbytes)


def run():
    import argparse
    parser = argparse.ArgumentParser(description="Run hydra master server")
    parser.add_argument('-host', dest='host', action='store')
    parser.add_argument('-port', dest='port', action='store')
    args = parser.parse_args()


app = Flask(__name__)


@app.before_first_request
def load_global_data():
    global hydra
    hydra = HydraMaster('', 0)


@app.route('/hydra/api/register', methods=['POST'])
def register_slave():
    request_json = request.get_json()
    host = request_json['host']
    port = request_json['port']
    reg_state, reg_msg = hydra.register_slave("{}:{}".format(host, port))
    if reg_state:
        return reg_msg
    return reg_msg


@app.route('/hydra/api/<string:key>', methods=['PUT'])
def put_key(key: str):
    value = request.get_data()
    hydra.set(key, value.decode('utf-8'))
    return "OK"


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=3000)


class HydraConnector:
    def __init__(self):
        pass

    def get_read_connection(self):
        pass

    def get_write_connection(self):
        pass
