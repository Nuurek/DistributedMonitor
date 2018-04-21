import random
from time import sleep

from communicator import Communicator


class Peer:

    def __init__(self, name: str, peers: dict):
        self.name = name

        self.peers = peers
        self.port = peers[self.name]['port']
        self.peers.pop(self.name)
        for peer in self.peers.values():
            peer['address'] += ':' + str(peer['port'])

        self.communicator = Communicator(self.port)

    def run(self):
        addresses = [peer['address'] for peer in self.peers.values()]

        for _ in range(1):
            sleep(random.random())
            self.communicator.broadcast_message(addresses, {
                'peer': self.name,
                'body:': 'lock'
            })

            sleep(random.random())
            self.communicator.broadcast_message(addresses, {
                'peer': self.name,
                'body:': 'unlock'
            })
