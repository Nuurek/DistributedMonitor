from collections import deque
from typing import Dict


class Token:
    def __init__(
            self, last_request_numbers, queue: deque,
            conditional_variable_queues: Dict[str, deque],
            signalled_queue: deque
    ):
        self.last_request_numbers = last_request_numbers
        self.queue: deque = queue
        self.conditional_variable_queues: Dict[str, deque] = conditional_variable_queues
        self.signalled_queue: Dict[str, deque] = signalled_queue

    @staticmethod
    def to_dict(instance) -> Dict:
        return {
            'last_request_numbers': instance.last_request_numbers,
            'queue': list(instance.queue),
            'conditional_variable_queues': {
                cv: list(instance.conditional_variable_queues[cv]) for cv in instance.conditional_variable_queues
            },
            'signalled_queue': list(instance.signalled_queue)
        }

    @staticmethod
    def from_dict(data: Dict):
        return Token(
            last_request_numbers=data['last_request_numbers'],
            queue=deque(data['queue']),
            conditional_variable_queues={
                cv: deque(data['conditional_variable_queues'][cv]) for cv in data['conditional_variable_queues']
            },
            signalled_queue=deque(data['signalled_queue'])
        )

    def __str__(self):
        return self.__repr__()

    def __repr__(self):
        return str(self.to_dict(self))
