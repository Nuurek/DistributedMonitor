from typing import Dict, List, Callable

from communicator import Communicator

Callback = Callable[[str, object], None]


class ChannelMessage:
    def __init__(self, sender: str, message: Dict):
        self.sender = sender
        self.message = message

    @staticmethod
    def to_dict(instance) -> Dict:
        return {
            'sender': instance.sender,
            'message': instance.message
        }

    @staticmethod
    def from_dict(data: Dict):
        return ChannelMessage(sender=data['sender'], message=data['message'])


class Channel:

    def __init__(self, communicator: Communicator, channel_name: str, sender: str):
        self.communicator = communicator
        self.channel_name = channel_name
        self.sender = sender

    def send_message(self, peer: str, message: Dict):
        channel_message = ChannelMessage(sender=self.sender, message=message)
        self.communicator.send_message(self.channel_name, peer, ChannelMessage.to_dict(channel_message))

    def broadcast_message(self, peers: List[str], message: Dict):
        channel_message = ChannelMessage(sender=self.sender, message=message)
        self.communicator.broadcast_message(self.channel_name, peers, ChannelMessage.to_dict(channel_message))

    def subscribe(self, callback: Callback):
        def unpack_callback(message):
            channel_message = ChannelMessage.from_dict(message)
            callback(channel_message.sender, channel_message.message)

        self.communicator.subscribe(self.channel_name, unpack_callback)
