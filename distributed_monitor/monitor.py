from distributed_monitor.communicator import Communicator, Channel
from distributed_monitor.mutex import DistributedMutex


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
                    return instance.mutex.is_locked()
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

        self.peers = config['peers']
        self.port = self.peers[self.peer_name]['port']
        self.peers.pop(self.peer_name)
        for peer in self.peers.values():
            peer['address'] += ':' + str(peer['port'])

        self.communicator = Communicator(self.port)
        self.channel = Channel(self.communicator, channel_name)
        self.mutex = DistributedMutex(self.channel, self.peer_name, **config)
