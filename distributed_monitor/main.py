import signal
import sys
import argparse
from subprocess import Popen


BASE_PORT_NUMBER = 50000

peers = []


def kill_peers(signal_id, frame):
    for peer in peers:
        peer.kill()
    sys.exit(0)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--number_of_peers', type=int, help="Number of peers to spawn")

    args = parser.parse_args(sys.argv[1:])

    print("Spawning peers")
    port_numbers = list(range(BASE_PORT_NUMBER, BASE_PORT_NUMBER + args.number_of_peers))
    for peer_id in range(args.number_of_peers):
        port_number = BASE_PORT_NUMBER + peer_id
        addresses = [f'tcp://localhost:{port}' for port in port_numbers if port != port_number]
        command = [
            'python',
            'peer.py',
            f'--name=peer{peer_id}',
            f'--port={port_number}',
            f'--addresses'
        ]
        command += addresses
        peers.append(Popen(command))

    signal.signal(signal.SIGINT, kill_peers)
    signal.pause()


if __name__ == '__main__':
    main()
