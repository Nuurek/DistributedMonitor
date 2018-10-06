import random
from collections import deque
from time import sleep
from typing import Dict, Any

from conditional_variable import ConditionalVariable
from monitor import DistributedMonitor, entry


class ProducerConsumerMonitor(DistributedMonitor):
    MAX = 3

    protected_data = ['queue']
    empty = ConditionalVariable()
    full = ConditionalVariable()

    def __init__(self, peer_name: str, config: dict):
        super().__init__(peer_name, config)

        self.queue = deque()
        self.task = config['peers'][self.peer_name]['task']
        self._log(self.task)

    @entry
    def enter(self, item):
        if len(self.queue) == self.MAX:
            self._log('Waiting for empty space')
            self.full.wait()

        sleep(random.random() * 2)
        self.queue.append(item)
        self._log(f'Produced {self.queue}')
        self.empty.signal()

    @entry
    def remove(self):
        if len(self.queue) == 0:
            self._log('Waiting for any item')
            self.empty.wait()

        sleep(random.random() * 2)
        item = self.queue.popleft()
        self._log(f'Consumed {self.queue}')
        self.full.signal()
        return item

    def run(self):
        while True:
            if self.task == 'producer':
                self.enter(random.randint(0, self.MAX))

            if self.task == 'consumer':
                self.remove()

            sleep(random.random() * 5)

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
