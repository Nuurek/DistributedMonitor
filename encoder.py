import json
from collections import deque

from mutex_token import Token


class Encoder(json.JSONEncoder):

    def default(self, o):
        if isinstance(o, deque):
            return list(o)
        if isinstance(o, Token):
            return o.__dict__


class Decoder(json.JSONDecoder):

    def __init__(self):
        super().__init__(object_hook=self.dict_to_object)

    def dict_to_object(self, d):
        if 'token' in d and d['token']:
            token = d['token']
            d['token'] = Token(
                last_request_numbers=token['last_request_numbers'],
                queue=deque(token['queue'])
            )

        return d
