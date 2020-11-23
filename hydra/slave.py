from typing import Dict
import urllib.request
import json
from socketserver import TCPServer
from socketserver import BaseRequestHandler


class HydraSlaveHandler(BaseRequestHandler):
    def handle(self) -> None:
        # noinspection PyTypeChecker
        hs: HydraSlave = self.server
        data = self.data
        print(data)


class HydraSlave(TCPServer):
    def __init__(self, server_addr, rhc, slave_url: str, master_url):
        self.url = slave_url
        self.master_url = master_url
        self.data: Dict = dict()

    def connect_to_master(self):
        req = urllib.request.Request("http://localhost:3000/hydra/api/register")
        req.add_header('Content-Type', 'application/json; charset=utf-8')
        jsondata = json.dumps({
            "host": "localhost",
            "port": 3001
        })
        jsondataasbytes = jsondata.encode('utf-8')  # needs to be bytes
        req.add_header('Content-Length', len(jsondataasbytes))
        response = urllib.request.urlopen(req, jsondataasbytes)

    def read_wal(self, wal):
        pass

    def set(self, k: str, v):
        self.data[k] = v
        print(self.data)

    def send_heartbeat(self):
        pass


def init():
    import argparse
    parser = argparse.ArgumentParser(description="Run hydra master server")
    parser.add_argument('-host', dest='host', action='store')
    parser.add_argument('-port', dest='port', action='store')
    parser.add_argument('-master', dest='master_url', action='store')
    return parser.parse_args()


def run():
    args = init()
    HOST, PORT, MASTER = args
    with HydraSlave((HOST, PORT), HydraSlave) as hydra_slave:
        hydra_slave.serve_forever()


if __name__ == "__main__":
    run()