import argparse
import logging
import select
import socket

def parse_args():
    parser = argparse.ArgumentParser(description="Non-blocking I/O receiver to evaluate scalability of data generator")

    # options
    parser.add_argument("--host", type=str, required=True,
                        help="IP address or hostname of data generator")
    parser.add_argument("-g", "--num-generators", type=int, required=True, help="Number of generators per port")
    parser.add_argument("-p", "--ports", type=lambda x: [int(p) for p in x.split(',')], required=True, help="List of ports to connect to")
    parser.add_argument('--loglevel',
                        choices=logging._nameToLevel.keys(),
                        default=logging.INFO,
                        help='Logging level')

    return parser.parse_args()

def connect_to_sockets(host, ports, num_generators):
    sockets = []
    for port in ports:
        for _ in range(num_generators):
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

            # Connect to the server
            sock.connect((host, port))
            sockets.append(sock)

    return sockets


def main(args):
    print(args)
    connected_sockets = connect_to_sockets(args.host, args.ports, args.num_generators)

    while True:
        ready_to_read, _, _ = select.select(connected_sockets, [], [])

        if ready_to_read:
            for sock in ready_to_read:
                sock.recv(8192)
        else:
            print("No sockets are ready to read")


if __name__ == "__main__":
    args = parse_args()

    main(args)
