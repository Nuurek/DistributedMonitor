import random
from time import sleep

from distributed_monitor.communicator import Communicator
from distributed_monitor.distributed_mutex import DistributedMutex


class Peer:

    def __init__(self, name: str, config: dict):
        self.name = name

        self.peers = config['peers']
        self.port = self.peers[self.name]['port']
        self.peers.pop(self.name)
        for peer in self.peers.values():
            peer['address'] += ':' + str(peer['port'])

        self.communicator = Communicator(self.port)
        self.mutex = DistributedMutex(self.communicator, self.name, **config)

    def run(self):
        addresses = [peer['address'] for peer in self.peers.values()]

        for _ in range(1):
            sleep(random.random())
            self.communicator.broadcast_message(addresses, {
                'peer': self.name,
                'body:': 'lock',
                'token': self.mutex.token
            })

            sleep(random.random())
            self.communicator.broadcast_message(addresses, {
                'peer': self.name,
                'body:': 'unlock',
                'token': self.mutex.token
            })
