"""
Leslie B. Klein
Date: 12/12/2015

This main module runs a bittorent client that connects to remote peers and 
listens for incoming connections from report peers.
if client.seeder == True: the client is a bittorrent seeder. 


# client is a seeder; its server is bound to (hostname, localserverport)
>> main_bt.py --seeder [-t <file.torrent>]  --hostname='127.0.0.1' --localserverport=16350

# client is a leecher 
# it connects to (hostname, remoteserverport) for downloading
# it listens on (hostname, localserverport) for uploading
>> main_bt.py --hostname='127.0.0.1' --remoteserverport=16329 --localserverport=16350 [-t <file.torrent>]           

# client connects to tracker (no hostname)
# client listens on ('127.0.0.1', localserverport) for uploading
>> main_bt.py --localserverport=16350 [-t <file.torrent>]  

The difference between the upload code and the download code has to do with opening a connection
and starting up the bittorrent protocol
uploader: receives a Handshake msg; sends a Handshake msg
downloader: sends a Handshake msg; receives a Handshake msg
If the peers at each end of the connection optionally send a bitfield msg, then there are 4 ways to start up the protocol.
After the start-up sequence, a stream reader and stream writer are attached to each peer object.
The peers are stored in self.active_peers {address: peer}, where the address key is (hostname, port).

The protocol state machine supports concurrent uploading and downloading in a single running program.
"""

import logging
import asyncio
import sys

from bt_utils import PORTS, HashError
from client import Client
from torrent_wrapper import TorrentWrapper
from arguments import torrent_file, seeder, hostname
from arguments import remoteserverport, localserverport
from arguments import remoteserverport1, remoteserverport2

# create logger for main_bt
logger = logging.getLogger('main_bt')
logger.setLevel(logging.INFO)

# create FileHandler which logs debug messages
fh = logging.FileHandler(filename='main_bt.log', mode='w')
fh.setLevel(logging.INFO) # INFO and DEBUG

# create console handler with a higher log level (or not)
ch = logging.StreamHandler()
ch.setLevel(logging.INFO) # INFO and DEBUG

# create formatter and add it to the handlers
formatter = logging.Formatter('{asctime} - {name} - {message}', style='{')
fh.setFormatter(formatter)
ch.setFormatter(formatter)

# add the handlers to the logger
logger.addHandler(fh)
logger.addHandler(ch)


@asyncio.coroutine
def main(client, port=remoteserverport, port1=remoteserverport1, port2=remoteserverport2):
    """
    client connects to TRACKER or peer=(hostname, remoteserverport)
    """

    if hostname:
        client.USE_TRACKER = False

    if client.seeder:
        # read files from file system into buffer
        logger.info('client is a seeder')
        try:
            client.read_files_into_buffer()
        except (FileNotFoundError, HashError) as e:
            logger.error(e.args)
            raise KeyboardInterrupt
        logger.info("finished reading files into buffer...")

    if not client.seeder:
        logger.info('client is a leecher')
        port_index = 0 # tracker port index
        while len(client.pbuffer.completed_pieces) != client.torrent.number_pieces:
            try:
                if client.USE_TRACKER:
                    # connect to tracker
                    success = client.connect_to_tracker(PORTS[port_index])  # blocking
                    port_index = (port_index + 1) % len(PORTS)
                else:
                    # connect directly to a peer (no tracker)
                    logger.info('leecher will connect directly to {}:{}'.\
                        format(hostname, port))
                    # remote_peer: (host, port)
                    # list_of_peers = [(hostname, port), (hostname, port1), (hostname, port2)]  
                    list_of_peers = [(hostname, rp) for rp in [port, port1, port2] if rp]
                    client._parse_active_peers_for_testing(list_of_peers)
                    success = True
            except KeyboardInterrupt as e:
                print(e.args)
                raise e
            except Exception as e:
                # try another tracker port
                logger.error(e.args)
                port_index = (port_index + 1) % len(PORTS)
                success = False
        
            if success:
                tasks_connect = [client.connect_to_peer(peer) \
                    for peer in client.active_peers.values() if peer] # a list of coros
                tasks_keep_alive = [client.send_keepalive(peer) \
                    for peer in client.active_peers.values() if peer] # a list of coros
                #tasks_close_quite_connections = [client.close_quiet_connections(peer) \
                #    for peer in client.active_peers.values()] # a list of coros
                try:
                    yield from asyncio.wait(tasks_connect+tasks_keep_alive)
                except Exception as e:
                    print(e.args)
                    raise KeyboardInterrupt

                # finished connecting to each peer, now...let's get some pieces
                while client.open_peers() and not client.all_pieces():
                    # at least 1 peer is open and 
                    # not all pieces are complete and 
                    if client.piece_cnts:
                        # open peer(s) have pieces that client needs
                        # piece_cnts is updated when client receives Have msg
                
                        # each task gets blocks for a piece from a distinct peer
                        result = client.select_piece() # stores [piece, set-of-peers] in attr
                        if result == 'success':
                            # for 1 piece at a time
                            #index, peers = client.selected_piece_peers
                            #tasks_get_piece = [client.get_piece(index, peer) for peer in peers]
                            # new code to support multiple pieces at a time...
                            tasks_get_piece = []
                            peers_assigned_to_a_task = set()
                            for index, peers in client.selected_piece_peers.items():
                                # each coro must use a distinct peer
                                tasks_get_piece += [client.get_piece(index, peer) for peer in peers \
                                    if peer not in peers_assigned_to_a_task]
                                peers_assigned_to_a_task = peers_assigned_to_a_task.union(peers)
                            try:
                                yield from asyncio.wait(tasks_get_piece+tasks_keep_alive)
                            except Exception as e:
                                logger.error(e.args)
                                raise KeyboardInterrupt
                    else:
                        # client.piece_cnts is empty
                        # client needs to read a Have msg to update client.piece_cnts
                        # then client can get next piece
                        logger.info('client waiting for Have msg')
                        yield from client.wait_for_peers_to_have_pieces()
                    
        # download complete
        logger.info('all pieces downloaded')
        if not hostname:
            client.TRACKER_EVENT='completed'
            client.connect_to_tracker(PORTS[port_index], numwant=0)
        
        # leecher sends out Not Interested to all peers
        # where bt_state[address].interested is True
        client.send_not_interested_to_all()

        # copy buffer to filesystem (keep data in buffer)
        # close file descriptors
        #client.write_buffer_to_file(reset_buffer=False) # files are closed after this completes


########################################

if __name__ == "__main__":
    """
    This script creates a bittorrent client and registers it with the event loop.
    It also creates a server and schedules it with the event loop.
    The top-level script:
    -- calls main(client, remoteserverport) to connect the client to 
    (hostname, remoteserverport) to download pieces.
    -- creates a listening server on (hostname, localserverport) that calls 
    client.handle_leecher each time it accepts a connection from some other client.
    """

    loop = asyncio.get_event_loop()
    loop.set_debug(enabled=True)

    # create client
    client = Client(TorrentWrapper(torrent_file), seeder=seeder)

    # schedule client
    loop.create_task(main(client, remoteserverport))

    # create and schedule server
    # server runs on (hostname, localserverport)
    port=localserverport
    server_coro = asyncio.start_server(client.handle_leecher, host='127.0.0.1', port=port)
    #server = loop.run_until_complete(server_coro) # schedule it
    server = loop.create_task(server_coro) # schedule it
    logger.info('server is running on {}:{}'.format(hostname, port))


    try:
        loop.run_forever()
    except KeyboardInterrupt as e:
        logger.info('server connections open: {}'.format(server.sockets))
        logger.info('closing server...')
    finally:
        # shutdown server (connection listener)
        client.shutdown()
        server.close()
        loop.run_until_complete(server.wait_closed())
        loop.close()
