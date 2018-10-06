from collections import deque
from threading import Lock, Event
from typing import Dict

from communicator import Communicator, Channel
from mutex_token import Token


class NotLockedException(Exception):
    pass


DEFAULT_MONITOR_CHANNEL = 'default-monitor-channel'


class MessageType:
    REQUEST = 'request'
    TOKEN = 'token'
    SYNCHRONIZE = 'synchronize'


RequestNumbers = Dict[str, int]


class DistributedMonitor:
    protected_data = []

    def __init__(self, peer_name: str, config: dict, channel_name: str=DEFAULT_MONITOR_CHANNEL):
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

    def lock(self):
        self._token_lock.acquire()
        if self.token:
            self._log('Already got token')
        else:
            self._request_numbers[self.peer_name] += 1
            self.channel.broadcast_message(self.peers, {
                'type': MessageType.REQUEST,
                'data': {
                    'request_number': self._request_numbers[self.peer_name]
                }
            })
            self._token_lock.release()
            self._log('Wait for token')
            self._token_acquired.wait()
            self._log('Received token')
            self._token_lock.acquire()

        self._is_locked = True
        self._token_lock.release()

    def unlock(self):
        self._token_lock.acquire()
        self.token.last_request_numbers[self.peer_name] = self._request_numbers[self.peer_name]

        not_queued_peers = set(self.peers) - set(self.token.queue)
        for peer in not_queued_peers:
            if self._request_numbers[peer] == self.token.last_request_numbers[peer] + 1:
                self.token.queue.append(peer)

        if len(self.token.queue) > 0:
            peer = self.token.queue.popleft()

            self.channel.send_message(peer, {
                'type': MessageType.TOKEN,
                'data': {
                    'token': self.token
                }
            })

            self.token = None

        self._is_locked = False
        self._token_lock.release()

    def is_locked(self):
        return self._is_locked

    def _on_message(self, sender: str, message):
        message_type_based_callbacks = {
            MessageType.REQUEST: self._on_request,
            MessageType.TOKEN: self._on_token
        }
        message_type = message['type']
        data = message['data']
        callback = message_type_based_callbacks[message_type]
        callback(sender, data)

    def _on_request(self, sender: str, data):
        print(f'[{sender}] -> [{self.peer_name}] REQUEST {data}')
        self._token_lock.acquire()
        sender_request_number = self._request_numbers[sender]
        sender_request_number = max([sender_request_number, data['request_number']])

        if self.token and not self.is_locked():
            last_sender_request_number = self.token.last_request_numbers[sender]
            if sender_request_number == last_sender_request_number + 1:
                self.channel.send_message(sender, {
                    'type': MessageType.TOKEN,
                    'data': {
                        'token': self.token
                    }
                })
                self.token = None
                self._token_acquired.clear()

        self._token_lock.release()

    def _on_token(self, sender: str, data):
        print(f'[{sender}] -> [{self.peer_name}] TOKEN {data}')
        self._token_lock.acquire()
        self.token = data['token']
        self._token_acquired.set()
        self._token_lock.release()

    def _log(self, message):
        print(f'[{self.peer_name}] {message}')


class NotDistributedMonitorSubclassException(Exception):
    pass


def entry(func):
    def method_wrapper(instance: DistributedMonitor, *args, **kwargs):
        if not issubclass(type(instance), DistributedMonitor):
            raise NotDistributedMonitorSubclassException()
        instance.lock()
        assert instance.token, instance.peer_name
        func(instance, *args, **kwargs)
        instance.unlock()
    return method_wrapper
