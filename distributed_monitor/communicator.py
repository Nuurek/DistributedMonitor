import signal

import threading

import zmq
import sys
from typing import Dict


class Communicator(object):

    def __init__(self, port: int, receive_callback=None):
        self.port = port
        self.context = zmq.Context()
        self.receive_socket = self.context.socket(zmq.PAIR)
        self.send_sockets: Dict[str, zmq.Socket] = {}

        signal.signal(signal.SIGINT, self._on_sig_int)

        self.receive_thread = threading.Thread(target=self._receive_messages)
        self.receive_thread.start()

    def send_message(self, address, message):
        if address not in self.send_sockets:
            self.send_sockets[address] = self.context.socket(zmq.PAIR)
            self.send_sockets[address].connect(address)
            self._log(f"Connected to {address}")

        self.send_sockets[address].send_json(message)
        self._log(f"Sent '{message}' to {address}")

    def broadcast_message(self, addresses, message):
        for address in addresses:
            self.send_message(address, message)

    def _receive_messages(self):
        self.receive_socket.bind(f'tcp://*:{self.port}')
        while True:
            message = self.receive_socket.recv_json()
            self._log(message)

    def _on_sig_int(self, signal, frame):
        self.context.term()
        self._log("Terminated context.")

        sys.exit(0)

    def _log(self, message):
        print(f'[{self.port}] {message}')
