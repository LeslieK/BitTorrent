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


This bittorrent client is written in python 3.4.3 using the asyncio library.
I am running on Windows 7 and use the Anaconda python distribution.
My goal was to use the Streams API and work at the highest possible level in the library.

To run the code:
>> python main_bt.py

or

>> python main_bt.py <a-torrent-file.torrent>

With no arguments, the program uses the included torrent as a default.

main_bt.py:
main_bt constructs a client, Client(<file.torrent>)
and calls the client to connect to peers and request blocks of pieces
If the remote peer requests pieces from the client, the client can upload the data requested to the peer.
(The upload part has not been tested but a lot of the code is there.)

client.py: 
defines 2 classes: Client and Peer
Client: stores lots of state data; implements the protocol using a download state machine (when the client is a leecher) and an upload state machine (when the client is a seeder)
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
tasks that are running until complete:
coros = [client.connect_to_peer(peer) for peer in client.active_peers.values()]
loop.run_until_complete(asyncio.wait(coros))

then...after connections are open...

# tasks that get a single piece by getting blocks from multiple peers (if possible)
coros = [client.get_piece(index, peer) for peer in peers]
loop.run_until_complete(asyncio.wait(coros))

I repeat getting a piece until all pieces are downloaded.
Sometimes I have to connect to the tracker to get more peers.
This happens if all peers close their connection with the client,
or peers that are open don't have the pieces the client needs,
and the client still needs pieces.

After the client has all pieces, it becomes a seeder (client.seeder = True)
It then can listen for incoming connections and serve them.

I am working on how to integrate the server with the other tasks that run concurrently in the loop.
