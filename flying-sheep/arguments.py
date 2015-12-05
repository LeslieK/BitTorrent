import argparse

# parse command line arguments
parser = argparse.ArgumentParser(description='run a bittorrent client')
parser.add_argument("-s", "--seeder", action='store_true', \
    help='client is a seeder')
parser.add_argument("-t", "--torrent_file", type=str, default="Mozart_mininova.torrent", \
    help='name of torrent file')

# hostname default is None
# default causes leecher to connect to tracker
parser.add_argument("-n", "--hostname", type=str, \
    help="ip address of a seeder peer")
parser.add_argument("-p", "--port", type=int, default=61329,\
    help="port number of remote peer on remote host")

args = parser.parse_args()

torrent_file = args.torrent_file
seeder = args.seeder
hostname = args.hostname
port = args.port

remote_peer = ('localhost', 61329)