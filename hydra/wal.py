
class WAL:
    def __init__(self, path: str):
        self.path: str = path
        self.id: int = 0

    def append(self, id: int, opp: str, k: str, v=None):
        with open(self.path, 'a') as f:
            pass


    def get_latest_id(self):
        pass