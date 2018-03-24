import os
import random
from time import sleep

from distributed_mutex import DistributedMutex

DEFAULT_EXCHANGE_NAME = 'clients'


class Client:
    def __init__(self, exchange_name=DEFAULT_EXCHANGE_NAME):
        self.name = f"Client-{os.getpid()}"
        print(f"[{self.name}] Hello world!")
        self.mutex = DistributedMutex(exchange_name)

    def run(self):
        for _ in range(3):
            sleep(random.random())
            self.mutex.lock()

            sleep(random.random())
            self.mutex.unlock()


if __name__ == '__main__':
    client = Client()
    client.run()
