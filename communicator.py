import signal
import sys
import threading
from typing import Dict, List, Callable

import zmq

from encoder import Encoder, Decoder

Callback = Callable[[str, str, object], None]


class MessageType:
    REQUEST = 'request'
    TOKEN = 'token'
    SYNCHRONIZE = 'synchronize'


class PeerAddressNotFoundException(Exception):
    pass


class InvalidMessageException(Exception):
    pass


class Communicator(object):

    def __init__(self, port: int, peer_addresses: Dict[str, str]):
        self.port = port
        self.peer_addresses = peer_addresses

        self.context = zmq.Context()
        self.receive_socket = self.context.socket(zmq.PULL)
        self.send_socket = self.context.socket(zmq.PUSH)

        signal.signal(signal.SIGINT, self._on_sig_int)

        self._receive_callbacks: Dict[str, Dict[str, List[Callback]]] = {}
        self.receive_thread = threading.Thread(target=self._receive_messages)
        self.receive_thread.start()

    def send_message(self, channel: str, tag: str, receiver: str, message: object):
        try:
            address = self.peer_addresses[receiver]
        except KeyError:
            raise PeerAddressNotFoundException()

        message = {
            'channel': channel,
            'tag': tag,
            'message': message
        }

        self.send_socket.connect(address)
        # self._log(f'Sending {message} to {receiver}')
        self.send_socket.send_json(message, cls=Encoder)
        # self._log('Sent message')
        # self.send_socket.recv()
        # self._log('Received ACK')
        self.send_socket.disconnect(address)

    def broadcast_message(self, channel: str, tag: str, addresses: List[str], message):
        for address in addresses:
            self.send_message(channel, tag, address, message)

    def subscribe(self, channel: str, tag: str, callback: Callback):
        if channel not in self._receive_callbacks:
            self._receive_callbacks[channel] = {}

        if tag not in self._receive_callbacks[channel]:
            self._receive_callbacks[channel][tag] = []

        self._receive_callbacks[channel][tag].append(callback)

    def _receive_messages(self):
        self.receive_socket.bind(f'tcp://*:{self.port}')
        while True:
            message = self.receive_socket.recv_json(cls=Decoder)
            # self._log(message)
            channel = message['channel']
            tag = message['tag']
            message = message['message']

            if channel in self._receive_callbacks:
                for callback in self._receive_callbacks[channel][tag]:
                    callback(message)

            # self.receive_socket.send_string("ACK")

    def _on_sig_int(self, signal, frame):
        self.context.term()
        self._log("Terminated context.")

        sys.exit(0)

    def _log(self, message):
        print(f'[{self.port}] {message}')


class Channel:

    def __init__(self, communicator: Communicator, channel_name: str, sender: str):
        self.communicator = communicator
        self.channel_name = channel_name
        self.sender = sender

    def send_message(self, tag: str, peer: str, message_type: str, message: object):
        packed_message = self._pack_message(message_type, message)
        self.communicator.send_message(self.channel_name, tag, peer, packed_message)

    def broadcast_message(self, tag: str, peers: List[str], message_type: str, message: object):
        packed_message = self._pack_message(message_type, message)
        self.communicator.broadcast_message(self.channel_name, tag, peers, packed_message)

    def subscribe(self, tag: str, callback: Callback):
        def unpack_callback(message):
            sender = message['sender']
            message_type = message['type']
            body = message['body']
            callback(sender, message_type, body)

        self.communicator.subscribe(self.channel_name, tag, unpack_callback)

    def _pack_message(self, message_type, message):
        return {
            'sender': self.sender,
            'type': message_type,
            'body': message
        }
