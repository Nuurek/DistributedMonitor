from communicator import Communicator, Channel
from mutex import DistributedMutex


class NotLockedException(Exception):
    pass


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

    def __init__(self, channel_name: str, peer_name: str, config: dict):
        self.peer_name = peer_name
        self.peers = list(config['peers'].keys())

        port = config['peers'][self.peer_name]['port']
        peer_addresses = {name: data['address'] + ':' + str(data['port']) for name, data in config['peers'].items()}
        self.communicator = Communicator(port, peer_addresses)

        self.channel = Channel(self.communicator, channel_name, self.peer_name)

        mutex_name = 'monitor-mutex'
        is_initial_token_holder = self.peer_name == config['initial_token_holder']

        self.mutex = DistributedMutex(mutex_name, self.channel, self.peer_name, self.peers, is_initial_token_holder)

    def lock(self):
        print('lock')

    def unlock(self):
        print('unlock')


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
