import signal
import sys
import argparse
from subprocess import Popen


BASE_PORT_NUMBER = 50000

clients = []


def kill_clients(signal_id, frame):
    for client in clients:
        client.kill()
    sys.exit(0)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--number_of_clients', type=int, help="Number of clients to spawn")

    args = parser.parse_args(sys.argv[1:])

    print("Spawning clients")
    port_numbers = list(range(BASE_PORT_NUMBER, BASE_PORT_NUMBER + args.number_of_clients))
    for client_id in range(args.number_of_clients):
        port_number = BASE_PORT_NUMBER + client_id
        addresses = [f'tcp://localhost:{port}' for port in port_numbers if port != port_number]
        command = [
            'python',
            'client.py',
            f'--name=client{client_id}',
            f'--port={port_number}',
            f'--addresses'
        ]
        command += addresses
        clients.append(Popen(command))

    signal.signal(signal.SIGINT, kill_clients)
    signal.pause()


if __name__ == '__main__':
    main()
