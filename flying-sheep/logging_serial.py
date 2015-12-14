"""
This module runs in a 'logging process' that serializes the log messages from the other processes.
In arguments.py, the default value for hostname cannot be the empty string if you want the leechers 
to talk to a seeder process (rather than a tracker).

if hostname, client.USE_TRACKER is set to False.
if not hostname, client.USE_TRACKER is True.
"""

import logging
import logging.handlers
import multiprocessing

import asyncio
from main_bt import Client, TorrentWrapper, main, logger
from arguments import remoteserverport, localserverport, seeder, hostname

def listener_configurer():
    root = logging.getLogger()
    h = logging.handlers.RotatingFileHandler(filename='bittorrent.log', mode='w', maxBytes=12**20)
    f = logging.Formatter('{asctime} - {processName} - {name} - {message}', style='{')
    h.setFormatter(f)
    root.addHandler(h)

def listener_process(queue, configurer):
    configurer()
    while True:
        try:
            record = queue.get()
            if record is None:
                break
            logger = logging.getLogger(record.name)
            logger.handle(record)
        except Exception:
            import sys, traceback
            print('Error!', file=sys.stderr)
            traceback.print_exc(file=sys.stderr)


# configure logger handler for each worker process
def worker_configurer(queue):
    h = logging.handlers.QueueHandler(queue)
    root = logging.getLogger()
    root.addHandler(h)
    root.setLevel(logging.INFO) # send all messages to queue

def worker_process(queue, configurer, localserverport, seeder=False, \
    remoteserverport=None, hostname='127.0.0.1', torrent_file='Mozart_mininova.torrent'):
    configurer(queue)
    name = multiprocessing.current_process().name
    print('Worker started: {}'.format(name))

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



def main_log():
    """
    This function sets up the following network:

    seeder: runs a server on (hostname, localserverport=16329)

    leecher_16350: runs a server on (hostname, localserverport=16350) and 
    connects to seeder (hostname, remoteserverport=16329)

    leecher_16351: connects to leecher_16350 by connecting to (hostname, remoteserverport=16350)

    leecher_16350 is in the middle of the sandwich. It concurrently downloads 
    from seeder and uploads to leecher_16351.
    """
    queue = multiprocessing.Queue(-1)
    listener = multiprocessing.Process(target=listener_process, args=(queue, listener_configurer))
    listener.start()

    seeder_16329 = multiprocessing.Process(target=worker_process, \
        args=(queue, worker_configurer, 16329, True))
    leecher_16350 = multiprocessing.Process(target=worker_process, \
        args=(queue, worker_configurer, 16350, False, 16329))
    leecher_16351 = multiprocessing.Process(target=worker_process, \
        args=(queue, worker_configurer, 16351, False, 16350)) 
    workers = [seeder_16329, leecher_16350, leecher_16351]
    for worker in workers:
        worker.start()
    for worker in workers:
        worker.join()
    queue.put_nowait(None) # sentinel
    listener.join()

#################### 
if __name__ == '__main__':
    try:
        main_log()
    except KeyboardInterrupt:
        pass