import random
from time import sleep

from distributed_monitor.monitor import DistributedMonitor


class ProducerConsumerMonitor(DistributedMonitor):
    MAX = 10

    protected_data = ['count']

    def __init__(self, channel_name: str, peer_name: str, config: dict):
        self.count = 0

        super().__init__(channel_name, peer_name, config)

    def enter(self):
        self.mutex.lock()
        self.count += 1
        self.mutex.unlock()

    def remove(self):
        self.mutex.lock()
        self.count -= 1
        self.mutex.unlock()

    def run(self):

        for _ in range(1):
            # sleep(random.random())
            self.enter()

            sleep(1)

            # sleep(random.random())
            self.remove()

        while True:
            pass
