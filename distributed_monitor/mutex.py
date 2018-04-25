from collections import deque
from typing import Dict

RequestNumbers = Dict[str, int]


class Token:

    def __init__(self, last_request_numbers, queue):
        self.last_request_numbers: RequestNumbers = last_request_numbers
        self.queue = queue

    def __str__(self):
        return self.__repr__()

    def __repr__(self):
        return str(self.__dict__)


class DistributedMutex:

    def __init__(self, communicator, peer_name, peers, initial_token_holder):
        self.communicator = communicator
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
