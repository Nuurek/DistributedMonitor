import argparse
import os
import random
import sys
from typing import List
from time import sleep

from communicator import Communicator


class Client:

    def __init__(self, name: str, port: int, addresses: List[str]):
        self.name = name
        self.addresses = addresses
        self.communicator = Communicator(port, self.addresses)

    def run(self):
        for _ in range(1):
            sleep(random.random())
            self.communicator.broadcast_message(self.addresses, {
                'client': self.name,
                'body:': 'lock'
            })

            sleep(random.random())
            self.communicator.broadcast_message(self.addresses, {
                'client': self.name,
                'body:': 'unlock'
            })


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--name', type=str, required=True, help="Name of the client")
    parser.add_argument('--port', type=int, required=True, help="Port number for receive socket to bind to")
    parser.add_argument('--addresses', type=str, required=True, nargs='+', help="Addresses of other clients")

    args = parser.parse_args(sys.argv[1:])

    client = Client(args.name, args.port, args.addresses)
    client.run()
