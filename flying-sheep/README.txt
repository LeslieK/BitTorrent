README.txt

Explains the files:
main_bt.py
client.py
piece_buffer.py
torrent_wrapper.py
bt_utils.py

These files replace the code in the files:
message_types.py
bt_utils.py

bittorent client: this application that runs as a process; it implements the bittorrent protocol
client: can refer either to the 'bittorrent client' application or can mean the download side (as opposed to the server side)
server: the part of the application that listens for incoming connections and handles each one.


This bittorrent client is written in python 3.4.3 using the asyncio library.
I am running on Windows 7 and use the Anaconda python distribution.
My goal was to use the Streams API and work at the highest possible level in the library.

Here are notes on how to run the code and the big picture of how it is architected.
But how can I inspect what it is doing?
To generate a serialized log file, run the code like this:
>> python logging_serial.py

This generates a log file that prints out all the logging.info in the modules.
The log filename is set in the listener_configurer() function.
Each instance of the application is a worker, identified as leecher_.... The number is the localserverport the server is running on. (--localserverport=16329) The arguments indicate whether the process is a seeder/leecher, its localserverport, and up to 3 remoteserverport.

To run the code from the main_bt module, see the notes at the top of that module.
For example,
to run the code as a leecher that connects to a Tracker, use:
>> python main_bt.py [-t <file.torrent>] 

To run the code as a seeder:
>> python main_bt.py --seeder --hostname='127.0.0.1' --localserverport=16329

With no arguments, the program uses the included torrent as a default.

main_bt.py:

constructs a client, Client(<file.torrent>)
creates a main task: main(client, port=remoterserverport, port1=remoteserverport1, port2=remoteserverport2)
schedules the main task: main(client, ...)
creates a server task: server(client.handle_leecher, host='127.0.0.1', port=localserverport)
schedules the server task
it then runs the loop forever

defines the function main(client, port, port1, port2)
This function creates tasks to connect the client to peers:
    tasks_connect = [client.connect_to_peer(peer) \
                    for peer in client.active_peers.values() if peer] # a list of coros
    yield from asyncio.wait(tasks_connect)


It then creates tasks to get pieces. First it selects the pieces to get and then creates the tasks to get them.
The core of this part is sort of like this:
    tasks_get_piece = [client.get_piece(index, peer) for peer in peers]
    yield from asyncio.wait(tasks_get_piece)


If client has no peers with pieces that it needs, client waits to get a Have msg from a peer so it can start requesting pieces again:
    yield from client.wait_for_peers_to_have_pieces()


client.py: 
defines 2 classes: Client and Peer
Client: stores lots of state data; implements the bittorrent protocol with a state machine that runs on each connection; doesn't matter whether the connection came in on the server-side or was initiated by the client. After the initial handshake/bitfield sequence, the 2 types of connections are in either of two states (2 or 20), and the state machines are identical.
Peer: a peer stores the reader and writer objects associated with the open connection
Client writes to peer (peer.writer.write()) and reads from peer (peer.reader.read())

piece_buffer.py: 
defines 1 class: PieceBuffer 
PieceBuffer stores the pieces that the client downloads.
Each piece is a row in the buffer.
The buffer is implemented as a dictionary of rows. Each row is implemented as an array.array
PieceBuffer is an attribute of the Client.

torrent_wrapper.py:
defines 1 class: TorrentWrapper
This class wraps the input torrent file.
TorrentWrapper is an attribute of the Client.

bt_utils.py:
helper functions and constants that can be configured

Big Picture:

# tasks that open all connections
    yield from asyncio.wait(tasks_connect)

then...after connections are open...

# tasks that get a piece(s) by getting blocks from multiple peers
    yield from asyncio.wait(tasks_get_piece)

The server doesn't wait for anything. It is created, scheduled, and runs forever.
It listens to incoming connections, processes the handshake/bitfield sequence, and then adds the peer to the active_peers data structure.

For the tracker:
I repeat getting a piece until all pieces are downloaded.
Sometimes the client has to connect to the tracker to get more peers.
This happens if all peers close their connection with the client,
or peers that are open don't have the pieces the client needs,
and the client still needs pieces.

After the client has all pieces, it becomes a seeder (client.seeder = True)
At this point, it just serves incoming connections.


