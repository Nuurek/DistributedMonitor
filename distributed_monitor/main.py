import sys
import argparse
from subprocess import Popen


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('number_of_clients', type=int, nargs=1, help="Number of clients to spawn")

    args = parser.parse_args(sys.argv[1:])

    print("Spawning clients")
    clients = []
    for _ in range(args.number_of_clients[0]):
        clients.append(Popen(['python', 'client.py']))

    for client in clients:
        client.wait()


if __name__ == '__main__':
    main()
