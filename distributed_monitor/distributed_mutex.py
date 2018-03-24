import signal

import os
import threading

import pika
import sys


class DistributedMutex(object):
    def __init__(self, exchange_name):
        self.exchange_name = exchange_name
        self.name = f"Client-{os.getpid()}"

        self.connection = pika.BlockingConnection(pika.ConnectionParameters())
        self.channel = self.connection.channel()

        self.channel.exchange_declare(exchange=self.exchange_name, exchange_type='fanout')

        message = f'{self.name} joined the channel'
        self._send_message(message)

        self.queue = self.channel.queue_declare(exclusive=True)
        self.queue_name = self.queue.method.queue

        self.channel.queue_bind(exchange=self.exchange_name, queue=self.queue_name)

        self.channel.basic_consume(self._on_message, queue=self.queue_name)

        signal.signal(signal.SIGINT, self._on_sig_int)
        self.thread = threading.Thread(target=self._wait_for_messages)
        self.thread.start()

    def _wait_for_messages(self):
        self.channel.start_consuming()
        self._log_message('Stopped consuming')

    def lock(self):
        self._send_message('Lock')

    def unlock(self):
        self._send_message('Unlock')

    def _send_message(self, message):
        self.channel.basic_publish(exchange=self.exchange_name, routing_key='', body=message)

    def _on_message(self, channel, method, properties, body):
        self._log_message(body.decode())
        pass

    def _on_sig_int(self, signal, frame):
        self._log_message("SIGINT")
        self.channel.stop_consuming()
        self.thread.join()
        self.connection.close()
        self._log_message("Killed connection")
        sys.exit(0)

    def _log_message(self, message):
        print(f'[{self.name}] {message}')
