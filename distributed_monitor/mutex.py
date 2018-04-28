from collections import deque
from typing import Dict, List

from distributed_monitor.communicator import MessageType, Channel
from distributed_monitor.mutex_token import Token

RequestNumbers = Dict[str, int]


class DistributedMutex:

    def __init__(self, name: str, channel: Channel, peer_name: str, peers: List[str], initial_token_holder: bool):
        self.name = name
        self.channel = channel
        self.channel.subscribe(self.name, self._on_message)
        self.peer_name = peer_name
        self.peers = peers
        self._request_numbers: RequestNumbers = {peer: 0 for peer in self.peers}
        self.peers.remove(self.peer_name)

        if initial_token_holder:
            self.token = Token(
                last_request_numbers={peer: 0 for peer in self.peers},
                queue=deque()
            )
        else:
            self.token = None

        self._is_locked = False

    def lock(self):
        self._request_numbers[self.peer_name] += 1
        request_number = self._request_numbers[self.peer_name]
        self.channel.broadcast_message(self.name, self.peers, MessageType.REQUEST, {
            'request_number': request_number
        })
        self._is_locked = True

    def unlock(self):
        self._is_locked = False

    def is_locked(self) -> bool:
        return self._is_locked

    def _on_message(self, sender, message_type, message):
        print(f'[{sender}] -> [{self.peer_name}] [{message_type}] {message}')
        if message_type == MessageType.REQUEST:
            request_number = message['request_number']
            self._request_numbers[sender] = max(self._request_numbers[sender], request_number)
