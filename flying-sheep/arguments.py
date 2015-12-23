import argparse

# parse command line arguments
parser = argparse.ArgumentParser(description='run a bittorrent client')
parser.add_argument("-s", "--seeder", action='store_true', \
    help='client is a seeder')
parser.add_argument("-t", "--torrent_file", type=str, default="Mozart_mininova.torrent", \
    help='name of torrent file')

# hostname default is None
# default causes leecher to connect to tracker
parser.add_argument("-n", "--hostname", type=str, default=None,\
    help="ip address of a seeder peer")
parser.add_argument("--remoteserverport", type=int,\
    help="port number of server on remote host")
parser.add_argument("--localserverport", type=int,\
    help="port number of server running on client")

parser.add_argument("--remoteserverport1", type=int, \
    help="port number of server on remote host")
parser.add_argument("--remoteserverport2", type=int, \
    help="port number of server on remote host")

args = parser.parse_args()

torrent_file = args.torrent_file
seeder = args.seeder
hostname = args.hostname
remoteserverport = args.remoteserverport
localserverport = args.localserverport
remoteserverport1 = args.remoteserverport1
remoteserverport2 = args.remoteserverport2