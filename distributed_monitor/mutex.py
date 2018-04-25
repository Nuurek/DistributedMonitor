from collections import deque
from typing import Dict

from distributed_monitor.communicator import Channel
from distributed_monitor.mutex_token import Token

RequestNumbers = Dict[str, int]


class DistributedMutex:

    def __init__(self, channel: Channel, peer_name: str, peers, initial_token_holder: str):
        self.channel = channel
        self.channel.subscribe(self._on_message)
        self.peer_name = peer_name
        self.peers = peers
        self._request_numbers: RequestNumbers = {peer: 0 for peer in self.peers}

        if peer_name == initial_token_holder:
            self.token = Token(
                last_request_numbers={peer: 0 for peer in self.peers},
                queue=deque()
            )
        else:
            self.token = None

        self._is_locked: bool = False

    def lock(self):
        self._is_locked = True

    def unlock(self):
        self._is_locked = False

    def is_locked(self) -> bool:
        return self._is_locked

    def _on_message(self, message):
        print(f'[{self.peer_name}] {message}')
