from typing import Dict
from flask import Flask, request
import urllib.request
import json

class HydraSlave:
    def __init__(self, slave_url: str, master_url):
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

    def set(self, k: str, v):
        self.data[k] = v
        print(self.data)

    def send_heartbeat(self):
        pass


app = Flask(__name__)
hydra = HydraSlave('', 0)
hydra.connect_to_master()


@app.route('/hydra/api/<string:key>', methods=['PUT'])
def put_key(key: str):
    value = request.get_data()
    hydra.set(key, value.decode('utf-8'))
    return "OK"


@app.route('/hydra/api/wal', methods=['POST'])
def wal_update():
    value = request.get_data()
    print(value)
    return "OK"


