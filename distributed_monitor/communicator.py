import signal
import sys
import threading
from typing import Dict, List, Callable

import zmq

from distributed_monitor.encoder import Encoder, Decoder


class Communicator(object):

    def __init__(self, port: int):
        self.port = port
        self.context = zmq.Context()
        self.receive_socket = self.context.socket(zmq.PAIR)
        self.send_sockets: Dict[str, zmq.Socket] = {}

        signal.signal(signal.SIGINT, self._on_sig_int)

        self._receive_callbacks: Dict[str, List[Callable]] = {}
        self.receive_thread = threading.Thread(target=self._receive_messages)
        self.receive_thread.start()

    def send_message(self, channel: str, address: str, message: object):
        if address not in self.send_sockets:
            self.send_sockets[address] = self.context.socket(zmq.PAIR)
            self.send_sockets[address].connect(address)
            self._log(f"Connected to {address}")

        message = {
            'channel': channel,
            'message': message
        }
        self.send_sockets[address].send_json(message, cls=Encoder)
        # self._log(f"Sent '{message}' to {address}")

    def broadcast_message(self, channel: str, addresses: List[str], message):
        for address in addresses:
            self.send_message(channel, address, message)

    def subscribe(self, channel: str, callback: Callable):
        if channel not in self._receive_callbacks:
            self._receive_callbacks[channel] = []

        self._receive_callbacks[channel].append(callback)

    def _receive_messages(self):
        self.receive_socket.bind(f'tcp://*:{self.port}')
        while True:
            message = self.receive_socket.recv_json(cls=Decoder)
            channel = message['channel']
            message = message['message']

            if channel in self._receive_callbacks:
                for callback in self._receive_callbacks[channel]:
                    callback(message)

            # self._log(message)

    def _on_sig_int(self, signal, frame):
        self.context.term()
        self._log("Terminated context.")

        sys.exit(0)

    def _log(self, message):
        print(f'[{self.port}] {message}')


class Channel:

    def __init__(self, communicator: Communicator, channel_name: str):
        self.communicator = communicator
        self.channel_name = channel_name

    def send_message(self, address: str, message):
        self.communicator.send_message(self.channel_name, address, message)

    def broadcast_message(self, addresses: List[str], message):
        self.communicator.broadcast_message(self.channel_name, addresses, message)

    def subscribe(self, callback: Callable):
        self.communicator.subscribe(self.channel_name, callback)
