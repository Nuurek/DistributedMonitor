import random
from collections import deque
from time import sleep
from typing import Dict, Any

from conditional_variable import ConditionalVariable
from monitor import DistributedMonitor, entry


class ProducerConsumerMonitor(DistributedMonitor):
    protected_data = ['queue']
    empty = ConditionalVariable()
    full = ConditionalVariable()

    AVERAGE_PRODUCTION_TIME = 3
    AVERAGE_CONSUMPTION_TIME = 2
    AVERAGE_SLEEP_TIME = 5
    MAX_ITEM = 10

    def __init__(self, peer_name: str, config: dict):
        super().__init__(peer_name, config)

        self.queue = deque()
        self.QUEUE_SIZE = int(config['queue_size'])
        self.task = config['peers'][self.peer_name]['task']

    @entry
    def enter(self, item):
        if len(self.queue) == self.QUEUE_SIZE:
            self.full.wait()

        sleep(random.random() * self.AVERAGE_PRODUCTION_TIME)
        self.queue.append(item)
        self._log('Produced:', list(self.queue))
        self.empty.signal()

    @entry
    def remove(self):
        if len(self.queue) == 0:
            self.empty.wait()

        sleep(random.random() * self.AVERAGE_CONSUMPTION_TIME)
        item = self.queue.popleft()
        self._log('Consumed:', list(self.queue))
        self.full.signal()
        return item

    def run(self):
        while True:
            if self.task == 'producer':
                self.enter(random.randint(0, self.MAX_ITEM))

            if self.task == 'consumer':
                self.remove()

            sleep(random.random() * self.AVERAGE_SLEEP_TIME)

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

    def _log(self, *messages):
        print(f'[{self.peer_name}]', *messages)
