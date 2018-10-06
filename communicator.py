import signal
import sys
import threading
from json import JSONEncoder, JSONDecoder
from typing import Dict, List, Callable

import zmq

Callback = Callable[[str, object], None]


class PeerAddressNotFoundException(Exception):
    pass


class InvalidMessageException(Exception):
    pass


class CommunicatorMessage:
    def __init__(self, channel: str, message: object):
        self.channel = channel
        self.message = message

    @staticmethod
    def to_dict(instance) -> Dict:
        return {
            'channel': instance.channel,
            'message': instance.message
        }

    @staticmethod
    def from_dict(data: Dict):
        return CommunicatorMessage(data['channel'], data['message'])


class Communicator:

    def __init__(
            self, port: int, peer_addresses: Dict[str, str],
            encoder_class: JSONEncoder = JSONEncoder, decoder_class=JSONDecoder
    ):
        self.port = port
        self.peer_addresses = peer_addresses
        self.encoder_class = encoder_class
        self.decoder_class = decoder_class

        self.context = zmq.Context()
        self.receive_socket = self.context.socket(zmq.PULL)
        self.send_socket = self.context.socket(zmq.PUSH)

        signal.signal(signal.SIGINT, self._on_sig_int)

        self._receive_callbacks: Dict[str, List[Callback]] = {}
        self.receive_thread = threading.Thread(target=self._receive_messages)
        self.receive_thread.start()

    def send_message(self, channel: str, receiver: str, message: Dict):
        try:
            address = self.peer_addresses[receiver]
        except KeyError:
            raise PeerAddressNotFoundException()

        communicator_message = CommunicatorMessage(channel, message)

        self.send_socket.connect(address)
        self.send_socket.send_json(CommunicatorMessage.to_dict(communicator_message), cls=self.encoder_class)
        self.send_socket.disconnect(address)

    def broadcast_message(self, channel: str, addresses: List[str], message):
        for address in addresses:
            self.send_message(channel, address, message)

    def subscribe(self, channel: str, callback: Callback):
        if channel not in self._receive_callbacks:
            self._receive_callbacks[channel] = []

        self._receive_callbacks[channel].append(callback)

    def _receive_messages(self):
        self.receive_socket.bind(f'tcp://*:{self.port}')
        while True:
            message = self.receive_socket.recv_json(cls=self.decoder_class)
            communicator_message = CommunicatorMessage.from_dict(message)

            if communicator_message.channel in self._receive_callbacks:
                for callback in self._receive_callbacks[communicator_message.channel]:
                    callback(communicator_message.message)

    def _on_sig_int(self, signal, frame):
        self.context.term()

        sys.exit(0)
