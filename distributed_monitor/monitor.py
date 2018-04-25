import random
from time import sleep

from distributed_monitor.communicator import Communicator
from distributed_monitor.mutex import DistributedMutex


class DistributedMonitor:

    def __init__(self, channel: str, peer_name: str, config: dict):
        self.peer_name = peer_name

        self.peers = config['peers']
        self.port = self.peers[self.peer_name]['port']
        self.peers.pop(self.peer_name)
        for peer in self.peers.values():
            peer['address'] += ':' + str(peer['port'])

        self.communicator = Communicator(self.port)
        self.mutex = DistributedMutex(self.communicator, self.peer_name, **config)

    def run(self):
        addresses = [peer['address'] for peer in self.peers.values()]

        for _ in range(1):
            sleep(random.random())
            self.communicator.broadcast_message(addresses, {
                'peer': self.peer_name,
                'body:': 'lock',
                'token': self.mutex.token
            })

            sleep(random.random())
            self.communicator.broadcast_message(addresses, {
                'peer': self.peer_name,
                'body:': 'unlock',
                'token': self.mutex.token
            })
