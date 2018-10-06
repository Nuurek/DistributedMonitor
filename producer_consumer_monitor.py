import random
from collections import deque
from time import sleep
from typing import Dict, Any

from monitor import DistributedMonitor, entry


class ProducerConsumerMonitor(DistributedMonitor):
    MAX = 10

    protected_data = ['queue']

    def __init__(self, peer_name: str, config: dict):
        super().__init__(peer_name, config)

        self.queue = deque()

    @entry
    def enter(self, item):
        sleep(random.random() * 2)
        self.queue.append(item)
        self._log(self.queue)

    @entry
    def remove(self):
        sleep(random.random() * 2)
        item = self.queue.popleft()
        self._log(self.queue)
        return item

    def run(self):
        while True:
            self.enter(random.randint(0, self.MAX))
            self.enter(random.randint(0, self.MAX))

            self.remove()

    @staticmethod
    def to_dict(data: Dict[str, Any]) -> Dict[str, any]:
        return {
            'queue': list(data['queue'])
        }

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> Dict[str, Any]:
        return {
            'queue': deque(data['queue'])
        }
