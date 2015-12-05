"""
Leslie B. Klein
Date:

This main module runs a client leecher.
The leecher does not listen for incoming connections. 
It only initiates connections to a peer and downloads pieces from it.
This leecher connects directly to (hostname, port) provided on the command line.

The remote peer (seeder) that uploads to this leecher is running main_bt.py in another process.
The remote peer is exercising its handle_leecher code.
This leecher is running connect_to_peer and get_next_piece code.
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
                if not hostname:
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
                    #loop.run_until_complete(asyncio.wait(tasks_connect+tasks_keep_alive)) # convert coros to tasks
                    yield from asyncio.wait(tasks_connect+tasks_keep_alive)
                except Exception as e:
                    print(e.args)
                    #client.shutdown() # send tracker event='stopped'; flush buffer to file system
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
                        #loop.run_until_complete(asyncio.wait(tasks_get_piece+tasks_keep_alive))
                        yield from asyncio.wait(tasks_get_piece+tasks_keep_alive)
                    except Exception as e:
                        print(e.args)
                        raise KeyboardInterrupt
                    
        # download complete
        print('all pieces downloaded')
        logging.debug('all pieces downloaded')
        client.TRACKER_EVENT='completed'
        client.connect_to_tracker(PORTS[port_index], numwant=0)
        client.send_not_interested_to_all()

        # copy buffer to filesystem (keep data in buffer)
        # close file descriptors
        client.write_buffer_to_file() # files are closed after this completes
        client.seeder = True  # affects shutdown






########################################

if __name__ == "__main__":

    loop = asyncio.get_event_loop()
    loop.set_debug(enabled=True)

    logger = logging.getLogger('asyncio')
    logging.basicConfig(filename="bittorrent.log", filemode='w', level=logging.DEBUG, format='%(asctime)s %(message)s')
    logging.captureWarnings(capture=True)

    # create client
    client = Client(TorrentWrapper(torrent_file), seeder=seeder)

    if seeder:
        # schedule client
        loop.create_task(main(client))

        # create and schedule server
        server_coro = asyncio.start_server(client.handle_leecher, host=hostname, port=port, loop=loop)
        server = loop.run_until_complete(server_coro) # schedule it
        print('seeder is running on {}:{}'.format(hostname, port))


    try:
        if seeder:
            loop.run_forever()
        else:
            loop.run_until_complete(main(client))
    except KeyboardInterrupt as e:
        print(e.args)
        logging.debug(e.args)
    except Exception as e:
        print(e.args)
        logging.debug(e.args)
    finally:
        if seeder:
            # shutdown server (connection listener)
            server.close()
            loop.run_until_complete(server.wait_closed())
        client.shutdown()  # tracker event='stopped' # flush buffer to file system (if necessary)
        loop.close()


#torrent_obj = TorrentWrapper("Mozart_mininova.torrent")
#torrent_obj = TorrentWrapper("Conversations with Google [mininova].torrent")
#torrent_obj = TorrentWrapper("Trigger Hippy Gorman Herring Freed Govrik - Live in Macon GA [mininova].torrent")
#torrent_obj = TorrentWrapper("A Free educational book for teaching english and english teachers [mininova].torrent")
#torrent_obj = TorrentWrapper("Noah Cohn - Devils Tongue [mininova].torrent")