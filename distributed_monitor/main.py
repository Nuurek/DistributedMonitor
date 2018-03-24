import signal
import sys
import argparse
from subprocess import Popen


clients = []


def kill_clients(signal, frame):
    for client in clients:
        client.kill()
    print("Killed clients")
    sys.exit(0)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('number_of_clients', type=int, nargs=1, help="Number of clients to spawn")

    args = parser.parse_args(sys.argv[1:])
    number_of_clients = args.number_of_clients[0]

    print("Spawning clients")
    for _ in range(number_of_clients):
        clients.append(Popen(['python', 'client.py']))

    signal.signal(signal.SIGINT, kill_clients)
    signal.pause()


if __name__ == '__main__':
    main()
