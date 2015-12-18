README.txt

Explains the files:
main_bt.py
client.py
piece_buffer.py
torrent_wrapper.py
bt_utils.py


bittorent client: this application; it implements the bittorrent protocol
client: can refer either to the 'bittorrent client' application or can mean the download side (as opposed to the server side)
server: the part of the application that listens for incoming connections and handles each one.


This bittorrent client is written in python 3.4.3 using the asyncio library.
I am running on Windows 7 and use the Anaconda python distribution.
My goal was to use the Streams API and work at the highest possible level in the library.

###########

Big Picture:

# tasks that open all connections
    yield from asyncio.wait(tasks_connect)

then...after connections are open...

# tasks that get a piece(s) by getting blocks from multiple peers
    yield from asyncio.wait(tasks_get_piece)

The server doesn't wait for anything. It is created, scheduled, and runs forever.
It listens to incoming connections, processes the handshake/bitfield sequence, and then adds the peer to the active_peers data structure.

###########

When the application starts up as a leecher (client.seeder=False; i.e., seeder is not present on command line):
The client gets peers from the tracker, connects to the peers, and downloads pieces.
Sometimes the client has to connect to the tracker multiple times to get more peers.
This happens if all peers close their connection with the client,
or peers that are open don't have the pieces the client needs,
and the client still needs pieces.

After the client has all pieces, it becomes a seeder (client.seeder = True. i.e., seeder is present on command line)
At this point, it just serves incoming connections.

When the application starts up as a seeder (a command line option --seeder), 
it reads the files on the file system into its buffer (client.pbuffer.buffer) rather than download them from the tracker.

############

To run the code from the main_bt module, see the notes at the top of that module.

For example, to run the code as a leecher that connects to a Tracker, use:

>> python main_bt.py [-t <file.torrent>] 
To store the downloaded pieces to files on the file system, uncomment the following line in the client.shutdown() method:

    # self.write_buffer_to_file()

Before writing the buffer to files, set the configuration variable bt_utils.ROOT_DIR to a directory on your filesystem. The next time the application runs, it can run as a seeder (rather than a leecher that must download pieces) and read the data from the files into the client's buffer. 


To run the code as a seeder:
>> python main_bt.py --seeder --hostname='127.0.0.1' --localserverport=16329

With no arguments, the program uses the included torrent as a default.

###############

-- main_bt.py:

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

-- client.py: 
defines 2 classes: Client and Peer
Client: stores lots of state data; implements the bittorrent protocol with a state machine that runs on each connection; doesn't matter whether the connection came in on the server-side or was initiated by the client. After the initial handshake/bitfield sequence, the 2 types of connections are in either of two states (2 or 20), and the state machines are identical.
Peer: a peer stores the reader and writer objects associated with the open connection
Client writes to peer (peer.writer.write()) and reads from peer (peer.reader.read())

-- piece_buffer.py: 
defines 1 class: PieceBuffer 
PieceBuffer stores the pieces that the client downloads.
Each piece is a row in the buffer.
The buffer is implemented as a dictionary of rows. Each row is implemented as an array.array
PieceBuffer is an attribute of the Client.

-- torrent_wrapper.py:
defines 1 class: TorrentWrapper
This class wraps the input torrent file.
TorrentWrapper is an attribute of the Client.

-- bt_utils.py:
helper functions and constants that can be configured

-- logging_serial.py
>> python logging_serial.py
Runs 4 instances of the bittorrent application; 1 instance per process
Each instance writes its log records to a Queue. The Queue serializes log records across all processes.
Runs an additional process to read the log records from the Queue and write them to a file.



