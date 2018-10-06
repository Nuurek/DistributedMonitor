from collections import deque
from threading import Lock, Event
from typing import Dict, Any

from channel import Channel
from communicator import Communicator
from messages import RequestMessage, TokenMessage
from monitor_token import Token


class NotLockedException(Exception):
    pass


DEFAULT_MONITOR_CHANNEL = 'default-monitor-channel'
RequestNumbers = Dict[str, int]


class DistributedMonitor:
    protected_data = []

    def __init__(self, peer_name: str, config: dict, channel_name: str = DEFAULT_MONITOR_CHANNEL):
        self.peer_name = peer_name
        all_peers = list(config['peers'].keys())
        self.peers = list(all_peers)
        self.peers.remove(self.peer_name)

        port = config['peers'][self.peer_name]['port']
        peer_addresses = {name: data['address'] + ':' + str(data['port']) for name, data in config['peers'].items()}
        self.communicator = Communicator(port, peer_addresses)

        self.channel = Channel(self.communicator, channel_name, self.peer_name)
        self.channel.subscribe(self._on_message)

        self._request_numbers: RequestNumbers = {peer: 0 for peer in all_peers}
        is_initial_token_holder = self.peer_name == config['initial_token_holder']
        if is_initial_token_holder:
            self.token = Token(
                last_request_numbers={peer: 0 for peer in all_peers},
                queue=deque()
            )
        else:
            self.token = None

        self._is_locked = False
        self._token_lock = Lock()
        self._token_acquired = Event()

    def _lock(self):
        self._token_lock.acquire()
        if self.token:
            self._log('Already got token')
        else:
            self._request_numbers[self.peer_name] += 1
            request_message = RequestMessage(request_number=self._request_numbers[self.peer_name])
            self.channel.broadcast_message(self.peers, RequestMessage.to_dict(request_message))
            self._token_lock.release()
            self._log('Wait for token')
            self._token_acquired.wait()
            self._log('Received token')
            self._token_lock.acquire()

        self._is_locked = True
        self._token_lock.release()

    def _unlock(self):
        self._token_lock.acquire()
        self.token.last_request_numbers[self.peer_name] = self._request_numbers[self.peer_name]

        not_queued_peers = set(self.peers) - set(self.token.queue)
        for peer in not_queued_peers:
            if self._request_numbers[peer] == self.token.last_request_numbers[peer] + 1:
                self.token.queue.append(peer)

        if len(self.token.queue) > 0:
            peer = self.token.queue.popleft()
            self._send_token(peer)
            self.token = None

        self._is_locked = False
        self._token_lock.release()

    def is_locked(self):
        return self._is_locked

    def _send_token(self, peer):
        protected_data = {attribute: getattr(self, attribute) for attribute in self.protected_data}
        token_message = TokenMessage(
            token=self.token,
            protected_data=self.to_dict(protected_data)
        )
        self.channel.send_message(peer, TokenMessage.to_dict(token_message))

    def _on_message(self, sender: str, message):
        message_type_based_callbacks = {
            RequestMessage.type: (RequestMessage, self._on_request),
            TokenMessage.type: (TokenMessage, self._on_token)
        }
        message_class, callback = message_type_based_callbacks[message['type']]
        message_object = message_class.from_dict(message)
        callback(sender, message_object)

    def _on_request(self, sender: str, message: RequestMessage):
        print(f'[{sender}] -> [{self.peer_name}] REQUEST {message}')
        self._token_lock.acquire()
        sender_request_number = self._request_numbers[sender]
        sender_request_number = max([sender_request_number, message.request_number])

        if self.token and not self.is_locked():
            last_sender_request_number = self.token.last_request_numbers[sender]
            if sender_request_number == last_sender_request_number + 1:
                self._send_token(sender)
                self.token = None
                self._token_acquired.clear()

        self._token_lock.release()

    def _on_token(self, sender: str, message: TokenMessage):
        print(f'[{sender}] -> [{self.peer_name}] TOKEN {message}')
        self._token_lock.acquire()
        self.token = message.token
        protected_data = self.from_dict(message.protected_data)
        for attribute, value in protected_data.items():
            setattr(self, attribute, value)
        self._token_acquired.set()
        self._token_lock.release()

    @staticmethod
    def to_dict(data: Dict[str, Any]) -> Dict[str, Any]:
        return data

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> Dict[str, Any]:
        return data

    def _log(self, message):
        print(f'[{self.peer_name}] {message}')


class NotDistributedMonitorSubclassException(Exception):
    pass


def entry(func):
    def method_wrapper(instance: DistributedMonitor, *args, **kwargs):
        if not issubclass(type(instance), DistributedMonitor):
            raise NotDistributedMonitorSubclassException()
        instance._lock()
        assert instance.token, instance.peer_name
        func(instance, *args, **kwargs)
        instance._unlock()

    return method_wrapper
