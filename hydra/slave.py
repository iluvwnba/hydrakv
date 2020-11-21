from configparser import ConfigParser


class HydraSlave:
    def __init__(self, slave_url: str, master_url):
        self.url = slave_url
        self.master_url = master_url

    def connect_to_master(self):
        pass

    def send_heartbeat(self):
        pass
