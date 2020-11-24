from cmd import Cmd
from typing import List, Tuple
from configparser import ConfigParser
from xmlrpc.client import ServerProxy


class HydraMasterConnector(Cmd):
    _supported_ops: List = ['set', 'delete']

    def __init__(self, m_host: str):
        self.intro = "Welcome to Hydra Writing CLI"
        self.prompt = 'hydra:> '
        super().__init__()
        self._master = m_host
        self.conn = ServerProxy('http://{}'.format(m_host))

    def do_exit(self, inp):
        print("Closing Hydra Shell")
        return True

    def do_set(self, arg):
        k, v = parse(arg)[0], parse(arg)[1]
        self.conn.set(k, v)

    def do_delete(self, arg):
        k = parse(arg)[0]
        self.conn.delete(k)

    def do_get(self, arg):
        k = parse(arg)[0]
        return self.conn.get(k)


class HydraSlaveConnector(Cmd):
    _supported_ops: List = ['set', 'delete']

    def __init__(self, s_hosts: List[str]):
        self.intro = "Welcome to Hydra Read CLI"
        self.prompt = 'hydra:> '
        super().__init__()
        #TODO Make random
        self._slave = s_hosts[0]
        self.conn = ServerProxy('http://{}'.format(self._slave))

    def do_exit(self, inp):
        print("Closing Hydra Shell")
        return True

    def do_get(self, arg):
        k, v = parse(arg)[0], parse(arg)[1]
        self.conn.set(k, v)

    def do_getall(self, arg):
        k = parse(arg)[0]
        self.conn.delete(k)


def parse(arg: str) -> Tuple:
    'Convert a series of zero or more numbers to an argument tuple'
    return tuple(arg.split(','))


def run():
    import argparse
    parser = argparse.ArgumentParser(description="Run Hydra CLI")
    parser.add_argument('-conn_type', dest='conn_type', action='store', help="read or write")
    parser.add_argument('-master', dest='master_host', action='store', help="<HOST>:<PORT>")
    parser.add_argument('-slave', dest='slave_hosts', action='store', nargs='+', help="Comma seperated <HOST>:<PORT>")
    args = parser.parse_args()

    config = ConfigParser()
    config.read('../config.ini')

    master: str = args.master_host or config.get('Master', 'RCP_Address')
    slaves: List[str] = args.slave_hosts or config.get('Slaves', 'RCP_Address_Q').split(',')

    if args.conn_type is "read":
        HydraSlaveConnector(slaves).cmdloop()
    else:
        HydraMasterConnector(master).cmdloop()


if __name__ == "__main__":
    run()
