from typing import Dict

from monitor_token import Token


class MonitorMessage:
    type = None

    def __init__(self, **kwargs):
        pass

    @classmethod
    def to_dict(cls, instance) -> Dict:
        return {
            'type': instance.type,
            'body': cls.body_to_dict(instance)
        }

    @staticmethod
    def body_to_dict(instance):
        raise NotImplementedError

    @classmethod
    def from_dict(cls, data: Dict):
        return cls(**cls.dict_to_kwargs(data['body']))

    @staticmethod
    def dict_to_kwargs(data: Dict):
        raise NotImplementedError

    def __repr__(self):
        return str(self.body_to_dict(self))


class RequestMessage(MonitorMessage):
    type = 'request'

    def __init__(self, request_number):
        super().__init__()
        self.request_number = request_number

    @staticmethod
    def body_to_dict(instance):
        return {
            'request_number': instance.request_number
        }

    @staticmethod
    def dict_to_kwargs(data: Dict):
        return {
            'request_number': data['request_number']
        }


class TokenMessage(MonitorMessage):
    type = 'token'

    def __init__(self, token: Token, protected_data: Dict):
        super().__init__()
        self.token = token
        self.protected_data = protected_data

    @staticmethod
    def body_to_dict(instance):
        return {
            'token': Token.to_dict(instance.token),
            'protected_data': instance.protected_data
        }

    @staticmethod
    def dict_to_kwargs(data: Dict):
        return {
            'token': Token.from_dict(data['token']),
            'protected_data': data['protected_data']
        }
