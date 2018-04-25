import argparse
import json
import sys

from distributed_monitor.peer import Peer

DEFAULT_PEERS_FILE_NAME = 'peers.json'

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--name', type=str, required=True, help="Name of the peer")
    parser.add_argument('--peers_file', type=str, help="Path to the config file", default=DEFAULT_PEERS_FILE_NAME)

    args = parser.parse_args(sys.argv[1:])

    with open(args.peers_file) as file:
        config = json.load(file)

    peer = Peer(args.name, config)
    peer.run()
