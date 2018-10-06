import random
from collections import deque

from communicator import Communicator, Channel
from mutex_token import Token


class NotLockedException(Exception):
    pass


DEFAULT_MONITOR_CHANNEL = 'default-monitor-channel'


class MessageType:
    REQUEST = 'request'
    TOKEN = 'token'
    SYNCHRONIZE = 'synchronize'


class DistributedMonitor:
    protected_data = []

    def __init_subclass__(cls, **kwargs):
        for attribute in cls.protected_data:
            protected_attribute_name = '_' + attribute

            def is_mutex_locked(instance: DistributedMonitor) -> bool:
                try:
                    hasattr(instance, 'mutex')
                    return True
                    # return instance.mutex.is_locked()
                except AttributeError:
                    return True

            def _getattr(self: DistributedMonitor):
                if not is_mutex_locked(self):
                    raise NotLockedException()
                return getattr(self, protected_attribute_name)

            def _setattr(self: DistributedMonitor, value):
                if not is_mutex_locked(self):
                    raise NotLockedException()
                setattr(self, protected_attribute_name, value)

            def _delattr(self: DistributedMonitor):
                if not is_mutex_locked(self):
                    raise NotLockedException()
                delattr(self, protected_attribute_name)

            _attr = property(_getattr, _setattr, _delattr)
            setattr(cls, attribute, _attr)

        super().__init_subclass__(**kwargs)

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

        is_initial_token_holder = self.peer_name == config['initial_token_holder']

        if is_initial_token_holder:
            self.token = Token(
                last_request_numbers={peer: 0 for peer in all_peers},
                queue=deque()
            )
        else:
            self.token = None

    def lock(self):
        print('lock')
        self.channel.send_message(random.choice(self.peers), {
            'type': MessageType.REQUEST,
            'data': {
                'id': self.peer_name
            }
        })

    def unlock(self):
        print('unlock')

    def _on_message(self, sender, message):
        print(f'[{sender}] -> [{self.peer_name}] {message}')


class NotDistributedMonitorSubclassException(Exception):
    pass


def entry(func):
    def method_wrapper(instance: DistributedMonitor, *args, **kwargs):
        if not issubclass(type(instance), DistributedMonitor):
            raise NotDistributedMonitorSubclassException()
        instance.lock()
        func(instance, *args, **kwargs)
        instance.unlock()
    return method_wrapper
