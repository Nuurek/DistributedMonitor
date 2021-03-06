import argparse
import json
import signal
import sys
from subprocess import Popen

from main import DEFAULT_PEERS_FILE_NAME


def main():
    peers = []

    def kill_peers(signal, frame):
        for _peer in peers:
            _peer.kill()
        sys.exit(0)

    signal.signal(signal.SIGINT, kill_peers)

    parser = argparse.ArgumentParser()
    parser.add_argument('--peers_file', type=str, help="Path to the config file", default=DEFAULT_PEERS_FILE_NAME)

    args = parser.parse_args(sys.argv[1:])

    with open(args.peers_file) as file:
        config: dict = json.load(file)

    print("Spawning peers")
    for name, peer in config['peers'].items():
        command = [
            'python',
            'main.py',
            f'--name={name}',
            f'--peers-file={args.peers_file}'
        ]
        peers.append(Popen(command))

    signal.pause()


if __name__ == '__main__':
    main()
