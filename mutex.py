from collections import deque
from threading import Lock, Event
from typing import Dict, List

from communicator import MessageType, Channel
from mutex_token import Token

RequestNumbers = Dict[str, int]


class DistributedMutex:

    def __init__(self, name: str, channel: Channel, peer_name: str, peers: List[str], initial_token_holder: bool):
        self.name = name
        self.channel = channel
        self.channel.subscribe(self.name, self._on_message)
        self.peer_name = peer_name
        self.peers = peers
        all_peers = self.peers + [peer_name]
        self._request_numbers: RequestNumbers = {peer: 0 for peer in all_peers}
        self.peers.remove(self.peer_name)

        if initial_token_holder:
            self.token = Token(
                last_request_numbers={peer: 0 for peer in all_peers},
                queue=deque()
            )
        else:
            self.token = None

        self._token_acquired = Event()
        self._internal_mutex = Lock()
        self._is_locked = False

    def lock(self):
        assert not self._is_locked

        self._log('Wants to enter CS')

        self._internal_mutex.acquire()

        if self.token:
            # Access the critical section
            self._log('Already has token')
        else:
            self._log('Need to sent REQUEST messages')
            self._request_numbers[self.peer_name] += 1
            request_number = self._request_numbers[self.peer_name]

            self.channel.broadcast_message(self.name, self.peers, MessageType.REQUEST, {
                'request_number': request_number
            })
            self._log('Sent REQUEST messages')

            self._internal_mutex.release()
            self._log('Waits for token acquired event')
            self._token_acquired.wait()
            self._log('Got token acquired event')
            self._internal_mutex.acquire()

        self._log('Entering CS')
        self._is_locked = True
        assert self.token is not None, self.peer_name

        self._internal_mutex.release()

    def unlock(self):
        assert self._is_locked
        assert self.token is not None

        self._log('Releasing CS')

        self._internal_mutex.acquire()

        self._is_locked = False

        self.token.last_request_numbers[self.peer_name] = self._request_numbers[self.peer_name]

        for peer in self.peers:
            queued_peers = set(self.token.queue)
            if self.token.last_request_numbers[peer] + 1 == self._request_numbers[peer] and peer not in queued_peers:
                self.token.queue.append(peer)

        if len(self.token.queue) > 0:
            peer = self.token.queue.popleft()

            self.channel.send_message(self.name, peer, MessageType.TOKEN, {
                'token': self.token
            })

            self._log(f'Sent TOKEN message to {peer} after releasing CS')
            self.token = None

        self._internal_mutex.release()

    def is_locked(self) -> bool:
        return self._is_locked

    def _on_message(self, sender, message_type, message):
        print(f'[{sender}] -> [{self.peer_name}] [{message_type}] {message}')

        self._internal_mutex.acquire()

        if message_type == MessageType.REQUEST:
            self._log(f'Got REQUEST from {sender}')
            request_number = message['request_number']
            self._request_numbers[sender] = max(self._request_numbers[sender], request_number)

            if (
                    self.token and
                    not self._is_locked and
                    self.token.last_request_numbers[sender] + 1 == self._request_numbers[sender]
            ):
                self.channel.send_message(self.name, sender, MessageType.TOKEN, {
                    'token': self.token
                })
                self._log(f'Sent TOKEN to {sender} after REQUEST message')
                self.token = None

        if message_type == MessageType.TOKEN:
            self.token = message['token']
            self._log(f'Acquired TOKEN from {sender}')
            self._token_acquired.set()
            self._log(f'Sent token acquired event')

        self._internal_mutex.release()

    def _log(self, message):
        print(f'[{self.peer_name}] {message}')
