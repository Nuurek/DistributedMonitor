from collections import deque


class Token:

    def __init__(self, last_request_numbers, queue):
        self.last_request_numbers = last_request_numbers
        self.queue: deque = queue

    def __str__(self):
        return self.__repr__()

    def __repr__(self):
        return str(self.__dict__)