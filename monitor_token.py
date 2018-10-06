from collections import deque
from typing import Dict


class Token:

    def __init__(self, last_request_numbers, queue):
        self.last_request_numbers = last_request_numbers
        self.queue: deque = queue

    @staticmethod
    def to_dict(instance) -> Dict:
        return {
            'last_request_numbers': instance.last_request_numbers,
            'queue': list(instance.queue)
        }

    @staticmethod
    def from_dict(data: Dict):
        return Token(
            last_request_numbers=data['last_request_numbers'],
            queue=deque(data['queue'])
        )

    def __str__(self):
        return self.__repr__()

    def __repr__(self):
        return str(self.to_dict(self))
