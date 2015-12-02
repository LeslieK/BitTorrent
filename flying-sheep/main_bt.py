
import logging
import asyncio
import sys

from bt_utils import PORTS
from client import Client
from torrent_wrapper import TorrentWrapper

@asyncio.coroutine
def main(client):
    #client = Client(TorrentWrapper(torrent_file), seeder=False)

    #server_coro = asyncio.start_server(client.handle_leecher, host='0.0.0.0', port=61328, loop=loop)
    #task_server = loop.create_task(server_coro)
    #task_server = loop.create_server(server_coro) # creates server and registers it with loop

    if not client.seeder:
        port_index = 0 # tracker port index
        while len(client.pbuffer.completed_pieces) != client.torrent.number_pieces:
            try:
                success = client.connect_to_tracker(PORTS[port_index])  # blocking  # mininova not working today 11/30
                #client._parse_active_peers_for_testing('85.104.237.222', 33382) # 85.104.237.222:33382 95.9.70.61:19130
                #list_of_peers = [('95.9.70.61', 19130), ('82.222.140.251', 27875), ('78.184.67.162', 56681)] # test code
                #client._parse_active_peers_for_testing(list_of_peers) # test code
            except KeyboardInterrupt as e:
                print(e.args)
                client.shutdown() # send tracker event='stopped'; flush buffer to file system
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
                #except KeyboardInterrupt as e:
                #    print(e.args)
                #    client.shutdown() # send tracker event='stopped'; flush buffer to file system
                #    raise e
                except Exception as e:
                    print(e.args)
                    client.shutdown() # send tracker event='stopped'; flush buffer to file system
                    raise e

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
                    #except KeyboardInterrupt as e:
                    #    print(e.args)
                    #    client.shutdown() # send tracker event='stopped'; flush buffer to file system
                    #    raise e
                    except Exception as e:
                        print(e.args)
                    
        # download complete
        print('all pieces downloaded')
        logging.debug('all pieces downloaded')
        print('client is a seeder')
        logging.debug('client is a seeder')
        client.seeder = True
        client.TRACKER_EVENT='completed'
        client.connect_to_tracker(PORTS[port_index], numwant=0)
        client.send_not_interested_to_all() # sends Not Interested msg to all open peers


    if client.seeder:
        # if buffer is empty, read files from file system into buffer
        # ...
        pass



########################################

if __name__ == "__main__":

    # get torrent file
    if len(sys.argv) == 1:
        torrent_file = "Mozart_mininova.torrent"
    elif len(sys.argv) > 1 and sys.argv[1].endswith('.torrent'):
        torrent_file = sys.argv[1]
    else:
        raise Exception('usage: >>python main_bt.py <torrentfile.torrent>')

    loop = asyncio.get_event_loop()
    loop.set_debug(enabled=True)

    logger = logging.getLogger('asyncio')
    logging.basicConfig(filename="bittorrent.log", filemode='w', level=logging.DEBUG, format='%(asctime)s %(message)s')
    logging.captureWarnings(capture=True)

    # create client
    client = Client(TorrentWrapper(torrent_file), seeder=False)

    # schedule client
    loop.create_task(main(client))

    # create and schedule server
    server_coro = asyncio.start_server(client.handle_leecher, host='0.0.0.0', port=61328, loop=loop)
    server = loop.run_until_complete(server_coro) # schedule it

    try:
        loop.run_forever()
    except KeyboardInterrupt as e:
        print(e.args)
        logging.debug(e.args)
    finally:
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