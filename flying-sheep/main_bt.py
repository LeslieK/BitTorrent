"""
Leslie B. Klein
Date: 12/6/2015

This main module runs a bittorent leecher or bittorrent seeder (aka server).
if client.seeder == True: server runs else leecher runs.

# listens to incoming connections
>> main_bt.py --seeder [-t <file.torrent>]  

# initiates connections; connects to tracker
>> main_bt.py [-t <file.torrent>]           

# initiates connections; does not connect to tracker
# connects directly to remote peer (aka seeder, aka server
>> main_bt.py --hostname='127.0.0.1' [-t <file.torrent>]           

The leecher does not listen for incoming connections. 
It only initiates connections to a peer and downloads pieces from it.

The remote peer (seeder/server) that uploads to this leecher is running main_bt.py in another process.
The remote peer is listening for a connection and then exercising the coroutine handle_leecher.
The leecher is running connect_to_peer and get_next_piece code.

main_bt.py does not run a bittorrent client that downloads pieces and uploads other pieces concurrently.
"""

import logging
import asyncio
import sys

from bt_utils import PORTS, HashError
from client import Client
from torrent_wrapper import TorrentWrapper
from arguments import torrent_file, seeder, hostname, port

@asyncio.coroutine
def main(client):

    if hostname:
        client.USE_TRACKER = False

    if client.seeder:
        # read files from file system into buffer
        print('client is a seeder')
        logging.debug('client is a seeder')
        try:
            client.read_files_into_buffer()
        except (FileNotFoundError, HashError) as e:
            print(e.args)
            raise KeyboardInterrupt from e
        print("finished reading files into buffer...")

    if not client.seeder:
        print('client is a leecher')
        port_index = 0 # tracker port index
        while len(client.pbuffer.completed_pieces) != client.torrent.number_pieces:
            try:
                if client.USE_TRACKER:
                    # connect to tracker
                    success = client.connect_to_tracker(PORTS[port_index])  # blocking
                else:
                    # connect directly to a peer (no tracker)
                    print('leecher will connect directly to {}:{}'.format(hostname, port))
                    list_of_peers = [(hostname, port)]  # remote_peer: (host, port)
                    client._parse_active_peers_for_testing(list_of_peers)
                    success = True
            except KeyboardInterrupt as e:
                print(e.args)
                raise e
            except Exception as e:
                # try another tracker port
                print(e.args)
                port_index = (port_index + 1) % len(PORTS)
                success = False
            
            port_index = (port_index + 1) % len(PORTS)  
        
            if success:
                tasks_connect = [client.connect_to_peer(peer) \
                    for peer in client.active_peers.values() if peer] # a list of coros
                tasks_keep_alive = [client.send_keepalive(peer) \
                    for peer in client.active_peers.values() if peer] # a list of coros
                #tasks_close_quite_connections = [client.close_quiet_connections(peer) \
                #    for peer in client.active_peers.values()]
                try:
                    yield from asyncio.wait(tasks_connect+tasks_keep_alive)
                except Exception as e:
                    print(e.args)
                    raise KeyboardInterrupt

                # finished connecting to each peer, now...let's get some pieces
                while client.open_peers() and not client.all_pieces() and client.piece_cnts:
                    # at least 1 peer is open and 
                    # not all pieces are complete and 
                    # open peer(s) may have pieces that client needs
                
                    # each task gets blocks for a piece from a distinct peer
                    result = client.select_piece() # stores [piece, set-of-peers] in attr
                    if result == 'success':
                        index, peers = client.selected_piece_peers
                        tasks_get_piece = [client.get_piece(index, peer) for peer in peers]
                    else:
                        # no pieces in open peers
                        # connect to tracker
                        break
                    try:
                        yield from asyncio.wait(tasks_get_piece+tasks_keep_alive)
                    except Exception as e:
                        print(e.args)
                        raise KeyboardInterrupt
                    
        # download complete
        print('all pieces downloaded')
        logging.debug('all pieces downloaded')
        if not client.seeder and not hostname:
            client.TRACKER_EVENT='completed'
            client.connect_to_tracker(PORTS[port_index], numwant=0)
        client.send_not_interested_to_all()

        # copy buffer to filesystem (keep data in buffer)
        # close file descriptors
        client.write_buffer_to_file() # files are closed after this completes
        client.seeder = True  # affects shutdown






########################################

if __name__ == "__main__":
    """
    This script lets the seeder (aka server) run forever.
    It lets the leecher run until main() completes, which happens when leecher downloads all pieces.
    """

    loop = asyncio.get_event_loop()
    loop.set_debug(enabled=True)

    logger = logging.getLogger('asyncio')
    logging.basicConfig(filename="bittorrent_seeder.log", filemode='w', level=logging.DEBUG, format='%(asctime)s %(message)s')
    logging.captureWarnings(capture=True)

    # create client
    client = Client(TorrentWrapper(torrent_file), seeder=seeder)

    if seeder:
        # schedule client
        loop.create_task(main(client))

        # create and schedule server
        server_coro = asyncio.start_server(client.handle_leecher, host='127.0.0.1', port=port, loop=loop)
        server = loop.run_until_complete(server_coro) # schedule it
        print('seeder is running on {}:{}'.format(hostname, port))


    if seeder:
        try:
            loop.run_forever()
        except KeyboardInterrupt as e:
            print(e.args)
        finally:
            # shutdown server (connection listener)
            server.close()
            loop.run_until_complete(server.wait_closed())
            loop.close()
    else:
        try:
            loop.run_until_complete(main(client))
        except KeyboardInterrupt as e:
            print('leecher {}'.format(e.args))
            logging.debug('leecher {}'.format(e.args))
        finally:
            client.shutdown()  # tracker event='stopped' # flush buffer to file system (if necessary)
            loop.close()
