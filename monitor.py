from collections import deque
from functools import partial
from threading import Event, Semaphore
from typing import Dict, Any

from channel import Channel
from communicator import Communicator
from conditional_variable import ConditionalVariable
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

        self._has_token = Event()
        self._semaphore = Semaphore()
        self._in_critical_section = Event()
        self._signalled = Event()

        conditional_variable_names = []
        for attribute_name in self.__dir__():
            attribute = getattr(self, attribute_name)
            if isinstance(attribute, ConditionalVariable):
                conditional_variable_names.append(attribute_name)
                conditional_variable: ConditionalVariable = attribute
                conditional_variable._initialize(self, attribute_name)
                conditional_variable.wait = partial(self.on_conditional_variable_wait, conditional_variable)
                conditional_variable.signal = partial(self.on_conditional_variable_signal, conditional_variable)

        is_initial_token_holder = self.peer_name == config['initial_token_holder']
        if is_initial_token_holder:
            self.token = Token(
                last_request_numbers={peer: 0 for peer in all_peers},
                queue=deque(),
                conditional_variable_queues={
                    name: deque() for name in conditional_variable_names
                },
                signalled_queue=deque()
            )
            self._has_token.set()
        else:
            self.token = None

    def _lock(self):
        self._semaphore.acquire()
        if self._has_token.is_set():
            self._in_critical_section.set()
        else:
            self._request_numbers[self.peer_name] += 1
            request_message = RequestMessage(request_number=self._request_numbers[self.peer_name])
            self.channel.broadcast_message(self.peers, RequestMessage.to_dict(request_message))
            self._semaphore.release()
            self._has_token.wait()
            self._semaphore.acquire()

        self._semaphore.release()

    def _unlock(self):
        self._semaphore.acquire()
        self.token.last_request_numbers[self.peer_name] = self._request_numbers[self.peer_name]

        self._respond_to_token_requests()

        self._in_critical_section.clear()
        self._semaphore.release()

    def _send_token(self, peer):
        self._has_token.clear()
        protected_data = {attribute: getattr(self, attribute) for attribute in self.protected_data}
        token_message = TokenMessage(
            token=self.token,
            protected_data=self.to_dict(protected_data)
        )
        self.token = None
        self.channel.send_message(peer, TokenMessage.to_dict(token_message))

    def _on_message(self, sender: str, message):
        self._semaphore.acquire()
        message_type_based_callbacks = {
            RequestMessage.type: (RequestMessage, self._on_request),
            TokenMessage.type: (TokenMessage, self._on_token)
        }
        message_class, callback = message_type_based_callbacks[message['type']]
        message_object = message_class.from_dict(message)
        callback(sender, message_object)
        self._semaphore.release()

    def _on_request(self, sender: str, message: RequestMessage):
        self._request_numbers[sender] = max([self._request_numbers[sender], message.request_number])

        if self._has_token.is_set() and not self._in_critical_section.is_set():
            last_sender_request_number = self.token.last_request_numbers[sender]
            if self._request_numbers[sender] == last_sender_request_number + 1:
                self._send_token(sender)

    def _on_token(self, sender: str, message: TokenMessage):
        self.token = message.token
        protected_data = self.from_dict(message.protected_data)
        for attribute, value in protected_data.items():
            setattr(self, attribute, value)
        self._has_token.set()
        self._signalled.set()
        self._in_critical_section.set()

    def on_conditional_variable_wait(self, conditional_variable: ConditionalVariable):
        self._semaphore.acquire()

        self.token.last_request_numbers[self.peer_name] = self._request_numbers[self.peer_name]

        cv_queue = self.token.conditional_variable_queues[conditional_variable.name]
        cv_queue.append(self.peer_name)

        self._respond_to_token_requests()

        self._in_critical_section.clear()
        self._semaphore.release()

        self._signalled.clear()
        self._signalled.wait()

    def on_conditional_variable_signal(self, conditional_variable: ConditionalVariable):
        self._semaphore.acquire()

        cv_queue = self.token.conditional_variable_queues[conditional_variable.name]
        if len(cv_queue) > 0:
            self.token.signalled_queue.append(self.peer_name)
            peer = cv_queue.popleft()
            self._send_token(peer)
            self._semaphore.release()
            self._has_token.wait()

        self._semaphore.release()

    def _respond_to_token_requests(self):
        not_queued_peers = set(self.peers) - set(self.token.queue)
        for peer in not_queued_peers:
            if self._request_numbers[peer] == self.token.last_request_numbers[peer] + 1:
                self.token.queue.append(peer)

        if len(self.token.signalled_queue) > 0:
            peer = self.token.signalled_queue.popleft()
            self._send_token(peer)
        elif len(self.token.queue) > 0:
            peer = self.token.queue.popleft()
            self._send_token(peer)

    @staticmethod
    def to_dict(data: Dict[str, Any]) -> Dict[str, Any]:
        return data

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> Dict[str, Any]:
        return data


class NotDistributedMonitorSubclassException(Exception):
    pass


def entry(func):
    def method_wrapper(instance: DistributedMonitor, *args, **kwargs):
        if not issubclass(type(instance), DistributedMonitor):
            raise NotDistributedMonitorSubclassException()
        instance._lock()
        func(instance, *args, **kwargs)
        instance._unlock()

    return method_wrapper
