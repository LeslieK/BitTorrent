﻿"""
Leslie B. Klein
Dec. 11, 2015

This module is a BitTorrent client without extensions.
It uploads pieces while it downloads pieces.

It uses python 3.4.3 with the asyncio library to achieve concurrency.
"""
from bcoding import bdecode, bencode
from collections import defaultdict, Counter

import array
import math
import bisect
import datetime
import os, socket
import requests
import asyncio

from bt_utils import my_peer_id, sha1_info, number_of_blocks
from bt_utils import rcv_handshake, make_handshake
from bt_utils import BTState, ChannelState
from bt_utils import HASH_DIGEST_SIZE
from bt_utils import ConnectionError, BufferFullError, ProtocolError
from bt_utils import ConnectionResetError, HashError
from bt_utils import bt_messages, bt_messages_by_name

from bt_utils import DEFAULT_BLOCK_LENGTH
from bt_utils import MAX_PEERS
from bt_utils import BLOCK_SIZE
from bt_utils import MAX_BLOCK_SIZE
from bt_utils import MAX_PIECES_TO_REQUEST  # used in client._get_next_piece()
from bt_utils import MAX_UNCHOKED_PEERS

from bt_utils import PORTS
from bt_utils import NUMWANT
from bt_utils import CONNECTION_TIMEOUT
from bt_utils import HANDSHAKE_ID
from bt_utils import KEEPALIVE_ID
from bt_utils import NOTSUPPORTED_ID
from bt_utils import ROOT_DIR

from piece_buffer import PieceBuffer

import logging

# create logger for client module
module_logger = logging.getLogger('main_bt.client')


#logging.basicConfig(filename="bittorrent.log", filemode='w', level=logging.DEBUG, format='%(asctime)s %(message)s')
#logging.captureWarnings(capture=True)

KEEPALIVE = bytes([0, 0, 0, 0])
CHOKE = bytes([0, 0, 0, 1]) + bytes([0])
UNCHOKE = bytes([0, 0, 0, 1]) + bytes([1])
INTERESTED = bytes([0, 0, 0, 1]) + bytes([2])
NOT_INTERESTED = bytes([0, 0, 0, 1]) + bytes([3])

class Peer(object):
    def __init__(self, torrent, address):
        self.logger = logging.getLogger('main_bt.client.Peer')
        self.logger.info('creating a Peer instance')
        self.torrent = torrent
        self.address = address # (ipv4, port)
        self.peer_id = None
        self.has_pieces = set()  # set of piece indices
        self.reader = None
        self.writer = None

        # bt_state = choked by client, interested in client
        self.bt_state = BTState() # choked = 1, interested = 0
        # state of leecher peer (leecher initiates connection to seeder)
        self.leecher_state = 0  # maybe can delete this

        # peer keepalive timer (reset when client receives msg from peer)
        self.timer = datetime.datetime.utcnow()
         
        # client keepalive timer (reset when client sends msg to peer)
        self._client_keepalive_timer = datetime.datetime.utcnow()

        # statistics
        self.num_bytes_uploaded = 0     # client.process_read_msg (client reads piece msg)
        self.num_bytes_downloaded = 0   # client.process_read_msg (client sends piece msg)
        
    def has_piece(self, piece_index):
        """
        True if peer has piece
        False otherwise
        """
        return piece_index in self.has_pieces 

class Client(object):
    """client uploads/downloads 1 torrent (1 or more files)"""

    def __init__(self, torrent, seeder=False):
        self.logger = logging.getLogger('main_bt.client.Client')
        self.logger.info('creating a Client instance')

        self.peer_id = my_peer_id()
        self.torrent = torrent
        self.seeder = seeder
        self.USE_TRACKER = True # set to False by main() when in trackerless mode

        self.files = self.torrent.file_meta
        self.HANDSHAKE = make_handshake(self.torrent.INFO_HASH, self.peer_id)  
        
        self.total_files_length = self.torrent.total_bytes
        self.piece_length = self.torrent.piece_length
        self.last_piece_length = self.torrent.last_piece_length

        self.last_piece_number_blocks = math.ceil(self.torrent.last_piece_length / BLOCK_SIZE)
        self.last_piece_size_of_last_block = self.torrent.last_piece_length - \
            (self.last_piece_number_blocks - 1) * BLOCK_SIZE
 
        self.num_bytes_uploaded = 0
        self.num_bytes_downloaded = 0
        self.num_bytes_left = self.torrent.total_bytes
        self.bitfield = self.init_bitfield() # sets all bits to 0; piece 0 is leftmost bit (MSB)

        self.pbuffer = PieceBuffer(torrent)        # PieceBuffer object
        self.buffer = self.pbuffer.buffer          # buffer attrib of PieceBuffer object; don't expose this

        # updated with each tracker response
        # active_peers: open and not-yet-tried-to-open peers
        self.active_peers = {}  # {address: peer}
        self.bt_state = {}      # {address: BTState()}  choked=1 interested=0
        self.channel_state = {} # {address: ChannelState()} open=0 state=0

        self.num_peers = 0  # len(self.active_peers)
        self.piece_to_peers = defaultdict(set)
        self.piece_cnts = Counter()
        self.closed_ips_cnt = 0

        # store last time tracker was contacted
        self.tracker_timer = datetime.datetime.utcnow()
        self.tracker_request_interval = datetime.timedelta(seconds=0)
        self.tracker_response = {}
        self.TRACKER_ID = None
        self.TRACKER_EVENT = 'started'

        # used by _get_next_piece
        self.selected_piece_peers = {} # {piece_index: {set of peers}}

        # used when uploading (server)
        self.number_unchoked_peers = 0

        # used by server (listens for incoming (leecher) connections)
        self.leecher_conn = {} # {address: peer} # leechers initiate connection to client

        # used by _open_leecher_connection
        # store server connection state
        self.server_channel_state = {} # {address: ChannelState()}
        # choked/not-choked by leecher
        # interested/not-interested in leecher
        self.server_bt_state = {} # {address: BTState(choked=1, interested=0)}
        
        self.closed_leechers_cnt = 0


        # used to read files from disk to cache (aka pbuffer.buffer)
        self._cache = array.array('B')
        self.output_fds = None # list of file descriptors for writing pieces

    def read_files_into_buffer(self):
        file_index = 0
        n_prev = 0
        for file in self.files:
            try:
                with open(os.path.join(ROOT_DIR, file['name']), mode='rb') as f:
                    n = self.torrent.file_boundaries_by_byte_indices[file_index]
                    nbytes = n - n_prev
                    self._cache.fromfile(f, nbytes)
                    file_index += 1
                    n_prev = n
            except Exception as e:
                #print('read_files_into_cache: {}'.format(e.args))
                self.logger('read_files_into_cache: {}'.format(e.args))
                raise KeyboardInterrupt from e
        self._copy_cache_into_buffer(cache_offset=0)

    def _copy_cache_into_buffer(self, cache_offset=0):
        begin = cache_offset
        for piece_index in range(self.torrent.number_pieces - 1):
            self.pbuffer._register_piece(piece_index)
            piece_length = self.torrent.piece_length
            # read piece from cache
            piece_bytes = self._cache[begin:begin+piece_length]  # array.array('B', b'123')
            # insert piece into buffer
            self.insert_piece(piece_index, piece_bytes)
            begin += piece_length
        # last iteration
        piece_index = self.torrent.LAST_PIECE_INDEX
        self.pbuffer._register_piece(piece_index)
        piece_length = self.torrent.last_piece_length
        # read piece from cache
        piece_bytes = self._cache[begin:begin+piece_length]  # array.array('B', b'123')
        # insert piece into buffer
        self.insert_piece(piece_index, piece_bytes)

        # set bit in piece bitfield
        self.update_bitfield(list(range(self.torrent.number_pieces)))

        # delete cache
        self._cache = None   

    def insert_piece(self, piece_index, piece_bytes):
        """
        seeder client uses this to copy piece from file cache into buffer
        piece_bytes: array.array
        """
        row = self.pbuffer.piece_info[piece_index]['row']
        self.pbuffer.buffer[row].frombytes(piece_bytes.tobytes())
        # check hash
        try:
            assert self.pbuffer._is_piece_hash_good(piece_index)
        except AssertionError as e:
            print('{} has bad hash'.format(piece_index))
            raise HashError from e
        self.pbuffer.piece_info[piece_index]['all_blocks_received'] = True
        self.pbuffer.piece_info[piece_index]['hash_verifies'] = True
        self.pbuffer.piece_info[piece_index]['bitfield'] = \
            (1 << (number_of_blocks(piece_index, self.torrent) + 1)) - 1 # all 1s
        self.pbuffer.completed_pieces.add(piece_index)

    def shutdown(self):
        """
        flush buffer to file
        shutdown each open peer
        close each open file
        send tracker event='stopped'
        """
        # flush buffer to file
        if not self.seeder and not self._is_bitfield_all_0s():
            # buffer has at least 1 piece
            self.logger.info('shutdown: flushing buffer...')

            #self.write_buffer_to_file() # -- for testing only

            self.logger.info('shutdown: flushed buffer')

        # close all open files
        if not self.seeder and self.output_fds: # leecher has opened file(s)
            self._close_fds()
            #print('closed all open files...')
            self.logger.info('closed all open files...')

        # shutdown client
        self.logger.info('client is shutting down...')

        success = all([self._close_peer_connection(peer) \
            for address, peer in self.open_peers().items()])

        if success:
            # successfully closed all open peers and leechers
            if self.USE_TRACKER:
                try:
                    self.TRACKER_EVENT = 'stopped'
                    r = self.connect_to_tracker(PORTS[0], numwant=0)
                except Exception as e:
                    #print('shutdown: error in connecting to tracker {}'.format(e.args))
                    self.logger.info('shutdown: error in connecting to tracker {}'.format(e.args))
            #print('shutdown: client completed shutdown')
            self.logger.info('shutdown: client completed shutdown')
            return True
        else:
            # client did not close all open peers and leechers
            self.logger.info('shutdown: client did not close all open peers/leechers')
            return False

    def _parse_active_peers_for_testing(self, address_list):
        """
        use this with a real peer instead of connecting to tracker
        """
        ptorrent = self.torrent
        for address in address_list:
            self.active_peers[address] = Peer(ptorrent, address)
            self.channel_state[address] = ChannelState()
            self.bt_state[address] = BTState()
        self.num_peers = len(self.active_peers)  # number of open and not-yet-connected-to peers
    
    def connect_to_tracker(self, port, numwant=NUMWANT):
        """send GET request; parse self.tracker_response
        
        event = 'started' on first request to connect
        event = 'stopped' when client shuts down gracefully
        event = 'completed' when download completes
        event = not specified, otherwise

        completed = False: all pieces have not been downloaded
        completed = True: all pieces downloaded

        resets client's tracker interval timer"""

        http_get_params = {
            'info_hash': self.torrent.INFO_HASH,
            'port': port,
            'uploaded': self.num_bytes_uploaded,
            'downloaded': self.num_bytes_downloaded,
            'left': self.num_bytes_left,
            'compact': 1,  # ipv4 only
            'peer_id': self.peer_id,
            'numwant': numwant
            }

        #print('connect_to_tracker: self.TRACKER_EVENT: {}'.format(self.TRACKER_EVENT))
        self.logger.info('connect_to_tracker: self.TRACKER_EVENT: {}'.format(self.TRACKER_EVENT))

        # blocking
        if self.TRACKER_EVENT:
            http_get_params['event'] = self.TRACKER_EVENT
        if self.TRACKER_ID:
            http_get_params['tracker id'] = self.TRACKER_ID
        try:
            r = requests.get(self.torrent.TRACKER_URL, params=http_get_params) # blocking
        except Exception as e:
            print(e.args)
            raise ConnectionError

        # reset client's tracker timer
        self.tracker_timer = datetime.datetime.utcnow()

        if not self.TRACKER_EVENT or self.TRACKER_EVENT == 'started':
            # don't parse if event is 'completed' or 'stopped'
            tracker_resp = r.content
            try:
                self.tracker_response = bdecode(tracker_resp)
            except Exception as e:
                print(e.args)
            success = self._parse_tracker_response()
            self.logger.info('{}: parsed tracker response'.format(success))
            #print('{}: parsed tracker response'.format(success))
            self.TRACKER_EVENT = None
            return success

    def _parse_tracker_response(self):
        """parses tracker response
        
        updates active_peers, bt_state, channel_state"""

        if 'failure reason' in self.tracker_response:
            self.logger.info('tracker failure: {}'.\
                format(self.tracker_response['failure reason']))
            return False
        elif 'warning message' in self.tracker_response:
            self.logger.info('tracker warning: {}'.\
                format(self.tracker_response['warning message']))
            return False
        else:
            self._parse_peers()  # updates active_peers, bt_state, channel_state
            self.tracker_request_interval = \
                datetime.timedelta(seconds=self.tracker_response.get('min interval', 
                                          self.tracker_response['interval']))
            if 'tracker id' in self.tracker_response:
                self.tracker_id = self.tracker_response['tracker id']
            return True
            
    def _parse_peers(self):
        """
        tracker response['peers']: (ip, port) formated in compact format

        input: byte string of peer addresses (6 bytes per address); tracker_response['peers']
        output: side effects: sets instance variables
        self.active_peers: {address: Peer(self.torrent, (ip, port)), ...}
        self.bt_state: {address: BTState() for address in dict_peers}
        self.channel_state: {address: ChannelState() for address in dict_peers}
        """
        peers = self.tracker_response['peers']
        dict_of_peers = {(ip, port_bytes[0]*256 + port_bytes[1]) : \
            Peer(self.torrent, (ip, port_bytes[0]*256 + port_bytes[1])) \
            for index in range(len(peers)//6) \
            for port_bytes in [peers[(index + 1)*4:(index + 1)*4 + 2]]\
            for ip in [socket.inet_ntoa(peers[index*4:(index + 1)*4])]}
        self.bt_state.update({address: BTState() for address in dict_of_peers \
            if address not in self.bt_state})
        self.channel_state.update({address: ChannelState() for address in \
            dict_of_peers if address not in self.channel_state})
        # after a peer channel transitions from open to close, it is removed from active_peers
        # initially, a peer is in active_peers with channel closed
        self.active_peers.update({address: dict_of_peers[address] \
            for address in dict_of_peers if address \
            not in self.active_peers})
        # number of open and not-yet-connected-to peers
        self.num_peers = len(self.active_peers)  

    def open_peers(self):
        """
        returns dict of all open peer connections
        """
        if not self.active_peers:
            return {}
        else:
            return {address:peer for address, peer in self.active_peers.items() \
                if self.channel_state[address].open}


    def reset_keepalive(self, peer):
        # timer stores the last time client wrote to peer
        peer._client_keepalive_timer = datetime.datetime.utcnow()

    def _close_peer_connection(self, peer):
        """clean-up after failed connection to peer"""
        self.logger.info('in _close_peer_connection...')
        if peer not in self.active_peers:
            self.logger.info('connection to peer already closed')
            return True
        self.logger.info('closing connection to {}'.format(peer.address))
        address = peer.address
        # check if peer is unchoked
        if not self.bt_state[address].choked:
            self.number_unchoked_peers -= 1
            peer.bt_state.choked = 1 # not necessary
        # close reader, writer
        peer.reader.set_exception(None)
        peer.writer.write(b'')
        peer.writer.close()
        # incr number of closed connections
        self.closed_ips_cnt += 1
        # close channel state
        del self.channel_state[address]
        del self.bt_state[address]
        # delete peer from client
        del self.active_peers[address]
        self.num_peers = len(self.active_peers)
        self.logger.info('client has {} unchoked peers'.format(self.number_unchoked_peers))
        self.logger.info('{}: cleaned up after closing connection'.format(address))
        return True

    @asyncio.coroutine
    def _open_peer_connection(self, peer):
        """
        sets channel_state[address]
        writes handshake msg
        reads handshake msg
        moves state machine to state 2
        throws ProtocolError

        write handshake
        read handshake
        write bitfield (if it has pieces)
        """
        address = peer.address
        self.logger.info('in _open_peer_connection to {}'.format(address))      

        # write handshake
        peer.writer.write(self.HANDSHAKE)
        self.channel_state[address].open = 1
        self.channel_state[address].state = 1
        self.logger.info('wrote Handshake to {}'.format(address))
        self.reset_keepalive(peer)

        # expect handshake msg
        try:
            msg = yield from peer.reader.readexactly(68)      # read handshake msg
        except (TimeoutError, OSError) as e:
            self.logger.error('{} ConnectionError'.format(address))
            raise ProtocolError from e
        except Exception as e:
            self.logger.error('{} Other Exception'.format(address))
            raise
        
        # check for handshake
        try:
            ident = self.check_handshake_msg(peer, bytearray(msg))
            if ident == HANDSHAKE_ID: # ident is False or HANDSHAKE_ID
                # update peer time (last time self received a msg from peer)
                peer.timer = datetime.datetime.utcnow()
                self.logger.info('successfully read Handshake from {}'.format(address))
        except ProtocolError as e:
            self.logger.error('did not receive Handshake msg from {}'.format(address, e.args))
            raise

        # self.channel_state[address] is in state 2, unless we write bitfield
        # write bitfield (if not all 0s)
        if not self._is_bitfield_all_0s():
            msg = self.make_bitfield_msg()
            peer.writer.write(msg)
            self.channel_state[address].state = 20
            self.logger.info('wrote Bitfield to {}'.format(address))
        else:
            self.channel_state[address].state = 2
        # update client timer (last time self wrote to peer)
        self.reset_keepalive(peer)
        

    @asyncio.coroutine
    def _open_leecher_connection(self, peer):
        """
        sets leecher_conn[address]
        sets server_bt_state[address]

        reads handshake
        writes handshake + bitfield
        moves state machine to state 2

        throws ProtocolError, Exception
        """
        address = peer.address
        self.logger.info('in _open_leecher_connection: accepted conn from {}'.format(address))

        # expect handshake
        try:
            msg = yield from peer.reader.readexactly(68)
        except (TimeoutError, OSError) as e:
            self.logger.error('{} ConnectionError'.format(address))
            raise ProtocolError from e
        except Exception as e:
            self.logger.error('{} Other Exception'.format(address))
            raise

        # check handshake
        ident = self.check_handshake_msg(peer, bytearray(msg))
        if ident == HANDSHAKE_ID: # ident is False or HANDSHAKE_ID
            # add peer to data structures
            self.leecher_conn[address] = peer  # add leecher to leechers
            self.server_channel_state[address] = 1  # conn is open
            # initial bt_state of server
            self.server_bt_state[address] = BTState(choked=1, interested=0) 
            # update peer time (last time self received a msg from peer)
            peer.timer = datetime.datetime.utcnow()
            self.logger.info('successfully read Handshake from {}'.format(address))
        else:
            self.logger.error('did not receive Handshake msg from {}'.format(address, e.args))
            raise ProtocolError

        # write handshake msg and bitfield msg (if not all 0s)
        # first write handshake
        peer.writer.write(self.HANDSHAKE)
        self.server_channel_state[address] = 2
        self.logger.info('wrote Handshake to {}'.format(address))
        # then write bitfield msg
        if not self._is_bitfield_all_0s():
            msg = self.make_bitfield_msg()
            peer.writer.write(msg)
            self.server_channel_state[address] = 20
            self.logger.info('wrote Bitfield to {}'.format(address))
        # update client timer (last time self wrote to peer)
        self.reset_keepalive(peer)
        


    def _length_of_last_piece(self):
        return self.total_files_length - self.piece_length * (self.torrent.number_pieces - 1)

    def _piece_length(self, piece_index):
        bytes_in_piece = self.piece_length if piece_index != self.torrent.LAST_PIECE_INDEX \
            else self.last_piece_length
        return bytes_in_piece

    def _number_of_bytes_left(self):
        return self.total_files_length - self.num_bytes_downloaded

    def create_dirs_for_pieces(self, multi=True):
        if multi:
            results = []
            for file in self.files:
                d = os.path.dirname(file['name'])
                if d and d not in results:
                    try:
                        os.makedirs(name=os.path.join(ROOT_DIR, d), exist_ok=False)
                    except OSError as e:
                        pass
                    results += [d]

    def open_files(self):
        """
        open files for writing pieces to file
        returns a list of file descriptors

        mode r: if file exists
        mode x: if file does not exist
        """
        # make dirs if necessary
        if self.torrent.is_multi_file():
            self.create_dirs_for_pieces()

        # open files
        for file in self.files:
            try:
                fds = [open(file['name'], mode='wb') for file in self.files]
            except Exception as e:
                raise KeyboardInterrupt from e
        return fds

    def write_buffer_to_file(self, reset_buffer=False):
        """
        write all pieces in buffer to the filesystem using the pathname
        """
        # open files
        self.output_fds = self.open_files()   

        # write entire buffer to file(s)
        try:
            for piece_index in self.pbuffer.piece_info.keys():
                self._write_piece_to_file(piece_index)
        except Exception as e:
            self.logger.error('write_buffer_to_file: {}'.format(e.args))
            raise KeyboardInterrupt from e
        finally:
            # close file descriptors
            self._close_fds()
                     

    def _write_piece_to_file(self, piece_index, reset_buffer=False):
        """
        row: indicates buffer row (i.e., piece) to read from
        start: byte position in piece indicating next byte to write
        reference: byte position in cache indicating beginning of a file
        offset: start - reference: offset into a file
        bytes_left: number of bytes in piece that still need to be written to file
        """
        row = self.pbuffer.piece_info[piece_index]['row']
        # write piece to 1 or more files
        bytes_left_to_write = self._piece_length(piece_index)               
        start = piece_index * self.torrent.piece_length
        file_index = bisect.bisect(self.torrent.file_boundaries_by_byte_indices, start)
        reference = 0 if file_index == 0 else self.torrent.file_boundaries_by_byte_indices[file_index-1]
        offset = start - reference 

        while bytes_left_to_write > 0:
            number_bytes_left_in_file = self.files[file_index]['length'] - offset
            if bytes_left_to_write > number_bytes_left_in_file:
                # bytes_left_to_write span multiple files
                number_bytes = number_bytes_left_in_file

                self._write(self.output_fds[file_index], offset, start, number_bytes, row)
                
                bytes_left_to_write -= number_bytes
                start += number_bytes
                file_index += 1
                offset = 0
            else:
                # bytes_left_to_write are in a single file
                self.logger.debug('about to write file {}: offset {} start {} nbytes {} row {}'\
                    .format(file_index, offset, start, bytes_left_to_write, row))

                self._write(self.output_fds[file_index], offset, start, bytes_left_to_write, row)

                bytes_left_to_write = 0
        if reset_buffer:
            self.pbuffer.reset(piece_index, free_row=True) # removes piece from buffer

    def _write(self, fd, offset, start, num_bytes, row):
        # begin is 0 or in the middle of a multi-file piece
        begin = start % self.torrent.piece_length  
        fd.seek(offset)
        bytes_ = self.buffer[row][begin:begin+num_bytes].tobytes()  
        fd.write(bytes_)
        fd.flush()
        fd.seek(0)

    def _close_fds(self):
        [fd.close() for fd in self.output_fds]
        return

    def _helper_comp(self, an_index):
        """returns the complement of an_index"""
        num_bytes=math.ceil(self.torrent.number_pieces / 8)
        max_i = num_bytes * 8 - 1
        return max_i - an_index

    def _is_bitfield_all_0s(self):
        return self.bitfield & (self.bitfield - 1) == 0

    def init_bitfield(self, list_of_pieces=None):
        """initialize bitfield; each bit represents a piece of the torrent
        
        leftmost bit (MSB): piece 0"""
        num_bytes = math.ceil(self.torrent.number_pieces / 8)
        field = 1 << (8 * num_bytes)
        if list_of_pieces:
            for index in (self._helper_comp(x) for x in list_of_pieces):
                field |= (1 << index)
        return field

    def update_bitfield(self, list_of_pieces):
        """update bitfield with new pieces
        bitfield: int
        return value includes leftmost 1: ignored when converted to bytes
        leftmost bit is piece 0
    
        ex: 0b10010 indicates piece 2 is present
        """
        if list_of_pieces:
            for index in (self._helper_comp(x) for x in list_of_pieces):
                self.bitfield |= (1 << index)
        return

    def int_to_bitstring(self):
        """bitfield is an int
        bitfield: 0b1---- ---- (ignore leftmost 1 after converting int)
        return a bitstring
        """    
        bitstring = bin(self.bitfield)[2:]
        num_bytes = math.ceil(len(bitstring) / 8)
        bitfield_bytes = (self.bitfield).to_bytes(num_bytes, byteorder='big')
        return bitfield_bytes[1:] # leftmost byte is 1 and is ignored

    def _int_to_4bytes(self, piece_index):
        payload = bin(piece_index)[2:].encode()
        num_payload_bytes = math.ceil(len(payload)/8)
        payload_bytes = (piece_index).to_bytes(num_payload_bytes, byteorder='big')
        return bytes(4 - num_payload_bytes) + payload_bytes

    def _4bytes_to_int(self, bytes):
        """bytes is a bit string of bytes: b'1234'

        return an integer
        """
        b = bytearray(bytes)
        return b[0]*256**3 + b[1]*256**2 + b[2]*256 + b[3]

    def get_indices(self, bitfield):
        b = bytearray(bitfield)
        b = ''.join([bin(x)[2:] for x in b])
        return {i for i, x in enumerate(b) if x == '1'}

    def make_bitfield_msg(self):
        """convert self.bitfield from int to a bit string"""
        num_bytes = math.ceil(self.torrent.number_pieces / 8)
        length = self._int_to_4bytes(num_bytes+1)
        ident = bytes([5])
        bitfield = self.int_to_bitstring()
        return length + ident + bitfield

    def make_have_msg(self, piece_index):
        """
        client makes msg after it downloads piece_index and it verifies hash
        """
        length = bytes([0, 0, 0, 5])
        ident = bytes([4])
        return length + ident + self._int_to_4bytes(piece_index)

    def make_cancel_msg(self, request_msg):
        buf = bytearray(request_msg)
        buf[4] = 8  # bytearray requires int values
        return bytes(buf)

    def make_request_msg(self, piece_index, begin_offset, block_len=DEFAULT_BLOCK_LENGTH):
        """only send request to a peer that has piece_index
        
        last block of last piece must be calculated; it is not the default
        """

        if piece_index == self.torrent.LAST_PIECE_INDEX:
            if begin_offset // BLOCK_SIZE == self.last_piece_number_blocks - 1:
                # last block of last piece
                block_len = self.last_piece_size_of_last_block

        length = b'\x00\x00\x00\x0d' # 13 bytes([0, 0, 0, 13])
        ident = b'\x06'
        index = self._int_to_4bytes(piece_index)
        begin = self._int_to_4bytes(begin_offset)
        num_bytes = self._int_to_4bytes(block_len)       
        return length + ident + index + begin + num_bytes

    def make_piece_msg_from_file(self, piece_index, begin_offset, block_size=MAX_BLOCK_SIZE):
        # convert piece_index to (file_index, offset) to get bytes from appropriate file
        file_index, offset = _piece_index_to_file_index(piece_index)

        ident = bytes[7]
        begin = _int_to_4bytes(begin_offset)
        index = _int_to_4bytes(piece_index)

        i = self.piece_indices[file_index].index(offset)  # returns the array index of the requested piece_index
        ablock = self.piece_bytes[file_index][i][begin_offset:begin_offset + block_size].tobytes() # ablock will really come from a file
        length = 9 + len(ablock)
        return length + ident + index + begin + ablock

    def make_piece_msg(self, index, begin, length_of_block):
        length = self._int_to_4bytes(9 + length_of_block)
        ident = bytes([7])
        piece_index = self._int_to_4bytes(index)
        offset = self._int_to_4bytes(begin)
        block = self._get_block_from_buffer(index, begin, length_of_block)
        return length + ident + piece_index + offset + block

    def _parse_handshake_msg(self, buf):
        """buf is a bytearray of the msg bytes"""
        msgd = {}
        msgd['pstrlen'] = buf[0]
        msgd['pstr'] = buf[1:20]
        msgd['reserved'] = buf[20:28]
        msgd['info_hash'] = buf[28:48]
        msgd['peer_id'] = buf[48:68]
        return msgd

    def _get_next_piece(self, num_peers=MAX_PIECES_TO_REQUEST):
        """
        yield [piece_index, {set of ips})

        1. find connected peers (self.channel_state[address].open == 1)
        2. peer.has_pieces is a set of piece indices
        3. get most rare piece that client doesn't already have
        """
        most_common = self.piece_cnts.most_common()
        # [(index, cnt), ..., (index, cnt)]

        if most_common:
            if len(most_common) <= num_peers:
                list_of_indices = most_common[:] # [(index, cnt), (index, cnt)]
            else:
                list_of_indices = most_common[-1:-num_peers-1:-1]

            # get all peers (ips) with rarest piece
            #index, cnt = list_of_indices[-1] # request 1 piece
            self.selected_piece_peers = {} # {index: {set of peers}}
            for index, _ in list_of_indices:
                # ips_with_least: {addr1, addr2, ... }
                ips_with_least = list(set(self.channel_state.keys()).intersection(self.piece_to_peers[index]))
                #if ips_with_least:
                #    self.selected_piece_peers += [index, \
                #        {self.active_peers[address] for address in ips_with_least}]
                #    self.logger.info('_get_next_piece: index {} peers {}'.\
                #        format(index, {p.address for p in self.selected_piece_peers[1]}))
                #    return 'success'
                if ips_with_least:
                    self.selected_piece_peers[index] = \
                        {self.active_peers[address] for address in ips_with_least}
                    if self.selected_piece_peers:
                        self.logger.info('_get_next_piece: index {} peers {}'.\
                                format(index, {p.address for p in self.selected_piece_peers[index]}))
                self.logger.info('self.selected_piece_peers: {}'\
                    .format({i: {p.address for p in ps} for i, ps in self.selected_piece_peers.items()}))
                return 'success'        
            else:
                # no open connections with pieces (all open connections have shut down)
                return 'no open connections'
        else:
            # no pieces left in self.piece_cnts
            self.logger.info('_get_next_piece: no pieces left in piece_cnts')
            self.logger.info('_get_next_piece: {} piece_cnts: {}  \
            most_common: {}'.format(address, self.piece_cnts, most_common))
            return 'need more pieces'

    def all_pieces(self):
        return len(self.pbuffer.completed_pieces) == self.torrent.number_pieces

    def _filter_on_not_complete():
            return {pindex: addresses for pindex, addresses in self.piece_to_peers.items() 
                    if pindex not in self.pbuffer.completed_pieces}

    def rcv_have_msg(self, peer, buf):
        """
        update client's representation of the sending peer's bitfield
        peer's bitfield indicates the pieces the peer has 
        """
        self.logger.info('in rcv_have_msg...')
        address = peer.address

        msgd = {}
        msgd['length'] = self._4bytes_to_int(buf[:4])
        msgd['id'] = buf[4]
        msgd['piece index'] = self._4bytes_to_int(buf[5:9])
        index = msgd['piece index']
        self.logger.info('rcv_have_msg: peer {} has piece {}'\
            .format(address, index))

        # update peer
        peer.has_pieces.add(index)
        self.piece_to_peers[index].add(address)
        if index not in self.pbuffer.completed_pieces:
            self.piece_cnts[index] += 1
        return msgd

    def rcv_bitfield_msg(self, peer, buf):
        """
        rcv bitfield from peer
        check it
        set client's representation of peer's bitfield
        throws Protocol error
        """
        self.logger.info('in rcv_bitfield_msg...')
        address = peer.address

        msgd = {}
        msgd['length'] = self._4bytes_to_int(buf[:4])
        msgd['id'] = buf[4]
        msgd['bitfield'] = buf[5:]

        # check bitfield
        try:
            self.check_bitfield_msg(msgd)
        except AssertionError as e:
            raise ProtocolError from e
        peer.bitfield = msgd['bitfield']
        peer.has_pieces = self.get_indices(msgd['bitfield'])

        # update data structures
        for index in peer.has_pieces:
            self.piece_to_peers[index].add(address)
            if index not in self.pbuffer.completed_pieces:
                self.piece_cnts[index] += 1
        return msgd

    def _parse_request_msg(self, buf):
        """
        parse request msg

        make piece msg to sender
        """
        msgd = {}
        msgd['length'] = self._4bytes_to_int(buf[:4])
        msgd['id'] = buf[4]
        msgd['index'] = self._4bytes_to_int(buf[5:9])
        msgd['begin'] = self._4bytes_to_int(buf[9:13])
        msgd['block_length'] = self._4bytes_to_int(buf[13:17])
        return msgd

    def rcv_piece_msg(self, buf):
        """
        get block from msg and store it in buffer

        if piece is complete:
          if hash is good:
            update bytes_downloaded
            update bitfield
            update piece/peer data structures
            send have msg
          if hash is bad:
            send interested to peer (if choked by peer)
            send request msg to peer
        """
        self.logger.info('in rcv_piece_msg...')
        msgd = self._parse_piece_msg(buf)
        length = msgd['length']
        ident = msgd['ident']
        index = msgd['index']
        begin = msgd['begin']
        block = msgd['block']

        if self.is_block_received(index, begin):
            self.logger.info('received duplicate block index {} begin: {}'\
                .format(index, begin))
            return 'already have block'
        try:
            result = self.pbuffer.insert_bytes(index, begin, block)
        except Exception as e:
            self.logger.error('rcv_piece_msg: {}'.format(e.args))
            raise e
        if result == 'done':
            # piece is complete and hash verifies

            # update num bytes downloaded and num bytes left
            bytes_in_piece = self._piece_length(index)
            self.num_bytes_downloaded += bytes_in_piece
            self.num_bytes_left -= bytes_in_piece
            # updates self.bitfield (bitmap for pieces in torrent)
            self.update_bitfield([index])
            # del piece from piece_cnts since piece is no longer needed
            del self.piece_cnts[index]
            # set offset to 0
            self.pbuffer.piece_info[index]['offset'] = 0

            # just received all the blocks for a piece
            # send Have message to all open peers that don't have piece
            try:
                self.send_have_msg(index)
            except Exception as e:
                raise e

        elif result == 'bad hash':
            # all blocks received and hash doesn't verify
            self.logger.info('bad hash index: {}'.format(index))
            self.pbuffer.reset(index)
            
        elif result == 'not done':
            # not all blocks received
            # old: increment offset after block bytes are received and inserted into buffer
            # old: self.pbuffer.piece_info[index]['offset'] = begin + length - 9 # lenth of block = length - 9
            # -- modified logic -- offset is updated in connect_to_peer/write Request
            pass
        return result


    def _get_block_from_buffer(self, index, offset, length_of_block):
        """
        returns requested block from buffer
        """
        row = self.pbuffer.piece_info[index]['row']
        block = self.pbuffer.buffer[row][offset:offset+length_of_block].tobytes()
        return block

    def send_not_interested_to_all(self):
        """
        client sends Not Interested to all open peers
        after download completes
        """
        for address, peer in self.open_peers().items():
            if self.bt_state[address].interested:
                self.bt_state[address].interested = 0
                try:
                    peer.writer.write(NOT_INTERESTED + b'') # send eof
                except Exception as e:
                    self.logger.error('error in sending Not Interested \
                    to {} {}'.format(address, e.args))
                self.logger.info('send_not_interested_to_all: {}'.format(address))


    @asyncio.coroutine
    def send_piece_msg(self, peer, msgd):
        """
        buf: bytearray(request msg)
        constructs Piece msg from Request msg
        writes Piece msg to peer
        return Piece msg
        """
        self.logger.info('in send_piece_msg...')
        index = msgd['index']
        begin = msgd['begin']
        block_length = msgd['block_length']
        msg = self.make_piece_msg(index, begin, block_length)
        try:
            peer.writer.write(msg)
            yield from peer.writer.drain()
        except Exception as e:
            self.logger.error('error in writing msg to {} index {} begin {}'\
                .format(peer.address, index, begin))
            raise e
        return msg
        
    def is_block_received(self, index, begin):
        """
        True if block from piece index has already been received
        test if bit associated with block in block_bitfield is a 1
        """
        if index in self.pbuffer.completed_pieces:
            return True
        if index in self.pbuffer.piece_info:
            block_bitfield = self.pbuffer.piece_info[index]['bitfield'] # an integer
            block_num = self.get_block_num(begin)
            return (block_bitfield >> block_num) % 2 == 1 # block bit is a 1
        else:
            # block is not part of completed piece and is not in buffer
            self.logger.error('block has never been received or even registered! index {} block {} offset {}'\
                .format(index, block_num, begin))
            return False

    def get_block_num(self, begin):
        return begin // BLOCK_SIZE

    def _get_next_offset(self, piece_index, offset):
        """
        helper method for _new_offset
        """
        block_num = self.get_block_num(offset)
        if piece_index == self.torrent.LAST_PIECE_INDEX:
            if block_num  == self.last_piece_number_blocks - 1:
                # current offset is last block of last piece
                next_offset = 0
            else:
                # offset is non-last-block of last piece
                next_offset = offset + BLOCK_SIZE
        else:
            # piece is not last piece
            next_offset = (offset + BLOCK_SIZE) % self.piece_length
        return next_offset

    def _new_offset(self, piece_index, begin):
        """
        begin: begin value from piece msg
        length: length value from piece msg
        returns next offset value for this piece_index 
        (for block not yet received) 

        fix this: need to store pending offsets in a set
        check if new offset is pending
        if yes: try to get another offset
        otherwise: done
        """
        def is_power_of_2(n):
            return n & (n - 1) == 0

        bitfield = self.pbuffer.piece_info[piece_index]['bitfield'] # an int

        curr_offset = begin
        next_offset = self._get_next_offset(piece_index, curr_offset)
        curr_block_num = self.get_block_num(curr_offset)
        next_block_num = self.get_block_num(next_offset)

        x = bitfield | (1 << curr_block_num)
        if is_power_of_2(x+1):
           # bitfield has one 0 (at curr_offset)
           # about to be all 1s if block at curr_offset is received
           return curr_offset
        
        if bitfield:
            # bitfield needs more than 1 block to be all 1s
            while bitfield & (1 << next_block_num):
                # next offset is for block that client already has
                # continue to find next_offset     
                next_offset = self._get_next_offset(piece_index, next_offset)
                next_block_num = self.get_block_num(next_offset)
            return next_offset
        # bitfield is all 0s
        return next_offset

    def _parse_piece_msg(self, buf):
        """
        buf: msg as a bytearray
        """
        msgd = {}
        msgd['length'] = self._4bytes_to_int(buf[:4])
        msgd['ident'] = buf[4]
        msgd['index'] = self._4bytes_to_int(buf[5:9])
        msgd['begin'] = self._4bytes_to_int(buf[9:13])
        msgd['block'] = buf[13:]
        return msgd

    def send_have_msg(self, piece_index):
        """
        this is not a coroutine, so I am not using yield from with writer
        """
        self.logger.info('in send_have_msg...')
        msg = self.make_have_msg(piece_index)
        for addressx, peerx in self.open_peers().items():
            if not peerx.has_piece(piece_index):
                try:
                    peerx.writer.write(msg)
                except Exception as e:
                    self.logger.error('rcv_piece_msg::done error \
                    in writing Have to {}'.format(addressx))
                    raise e
                self.logger.info('done successfully wrote Have msg to {}'.format(addressx))
            else:
                # peer already has piece
                pass

    def send_choke_msg(self, peer):
        try:
            peer.writer.write(CHOKE)
        except Exception as e:
            self.logger.error('send_choke_msg: {}'.format(e.args))
            raise e

    def send_unchoke_msg(self, peer):
        try:
            peer.writer.write(UNCHOKE)
        except Exception as e:
            self.logger.error('send_unchoke_msg: {}'.format(e.args))
            raise e

    def check_handshake_msg(self, peer, buf):
        """
        buf: bytearray(msg)
        returns False or HANDSHAKE_ID
        """
        self.logger.info('in check_handshake_msg...')
        try:
            assert buf[1:20].decode() == 'BitTorrent protocol'
        except UnicodeDecodeError as e:
           return False
        except AssertionError as e:
            return False
        else:
            try:
                assert buf[0] == 19
            except AssertionError as e:
                self.logger.error("received handshake msg with length {}; \
                should be 19".format(buf[0].decode()))

                raise ProtocolError("received handshake msg with length {}; \
                should be 19".format(buf[0].decode()))
            #  handshake message
            msgd = self._parse_handshake_msg(buf)
            try:
                # check info_hash
                assert msgd['info_hash'] == self.torrent.INFO_HASH
            except AssertionError as e:
                self.logger.error("peer is not part of torrent: \
                expected hash: {}".format(self.torrent.INFO_HASH))

                raise ProtocolError("peer is not part of torrent: \
                expected hash: {}".format(self.torrent.INFO_HASH))

            # Note: check peer_id
            # check that peer.peer_id from tracker_response['peers'] (dictionary model)
            # matches peer.peer_id in handshake (msgd['peer_id'])
            # tracker response uses compact mode so peer.peer_id is not in tracker_response

            # set peer_id of peer
            peer.peer_id = msgd['peer_id']
            # reset peer timer
            peer.timer = datetime.datetime.utcnow()

            return HANDSHAKE_ID

    def check_request_msg(self, buf):
        """
        buf: bytearray(msg) where msg is b'...'
        msgd: dictionary containing values in msg
        throws ProtocolError, Exception
        """
        msgd = self._parse_request_msg(buf)
        length = msgd['length']
        ident = msgd['id']
        index = msgd['index']
        begin = msgd['begin']
        block_length = msgd['block_length']
        # check that begin + block_length <= piece_length
        if index == self.torrent.LAST_PIECE_INDEX:
            # last piece
            piece_length = self.last_piece_length
        else:
            piece_length = self.piece_length
        try:
            assert begin + block_length <= piece_length
        except AssertionError as e:
            self.logger.error('check_request_msg: {}'.format(e.args))
            raise ProtocolError('check_request_msg: begin + block_length \
            exceeds piece length index {}'.format(index))
        except Exception as e:
            self.logger.error(e.args)
        return msgd

    def check_bitfield_msg(self, msgd):
        """
        msgd: dictionary containing values in msg
        throws AssertionError
        """
        self.logger.info('in check_bitfield...')
        bitfield = msgd['bitfield']  # bytearray
        number_padding_bits = 8 - self.torrent.number_pieces % 8
        try:
            assert bitfield[-1] >> number_padding_bits << number_padding_bits == bitfield[-1]
        except AssertionError as e:
            self.logger.error('bitfield padding bits contain at least one 1 index {}'\
                .format(msgd['index']))
            raise e

    #@asyncio.coroutine
    #def process_read_server(self, peer, msg):
    #    """
    #    process msg that is received by server
    #    """
    #    pass

    @asyncio.coroutine
    def process_read_msg(self, peer, msg):
        """process incoming msg from peer - protocol state machine
        
        peer: peer instance
        msg: bittorrent msg
        throws ProtocolError, BufferFullError
        """

        self.logger.info('in process_read_msg...')
        if not msg:
            return

        buf = bytearray(msg)
        address = peer.address

        ident = buf[4]

        if ident == 0:
            #  read choke message
            length = self._4bytes_to_int(buf[0:4])
            try:
                assert length == 1
            except AssertionError:
                raise ProtocolError("Choke: bad length  received: {} expected: 1"\
                                        .format(length))
            self.bt_state[address].choked = 1  # peer chokes client
            self.logger.info('client successfully read {} from {} in state: {}'\
                 .format(bt_messages[ident], address, self.channel_state[address].state))

            if self.channel_state[address].state == 6:
                self.channel_state[address].state = 5
            elif self.channel_state[address].state == 7:
                self.channel_state[address].state = 71
            elif self.channel_state[address].state == 9:
                self.channel_state[address].state = 10
            elif self.channel_state[address].state == 50:
                self.channel_state[address].state = 4
            elif self.channel_state[address].state == 8:
                self.channel_state[address].state = 81
            elif self.channel_state[address].state == 21:
                self.channel_state[address].state = 22
            else:
                pass   # peer chokes client
        elif ident == 1:
            #  read unchoke message
            length = self._4bytes_to_int(buf[0:4])
            try: 
                assert length == 1
            except AssertionError:
                raise ProtocolError("Unchoke: bad length  received: {} expected: 1"\
                                        .format(length))

            self.bt_state[address].choked = 0  # peer unchoked client
            self.logger.info('client successfully read {} from {} in state: {}'\
                 .format(bt_messages[ident], address, self.channel_state[address].state))
            if self.channel_state[address].state == 3:
                self.channel_state[address].state = 50
            elif self.channel_state[address].state == 5:
                self.channel_state[address].state = 6
            elif self.channel_state[address].state == 10:
                self.channel_state[address].state = 9
            elif self.channel_state[address].state == 4:
                self.channel_state[address].state = 50
            elif self.channel_state[address].state == 71:
                # when waiting to recv Piece but get Unchoke, go back to Req
                self.channel_state[address].state = 6  
            elif self.channel_state[address].state == 81:
                self.channel_state[address].state = 8
            elif self.channel_state[address].state == 20:
                self.channel_state[address].state = 21
            elif self.channel_state[address].state == 22:
                self.channel_state[address].state = 21
            elif self.channel_state[address].state == 2:
                self.channel_state[address].state = 21
        elif ident == 2:
            #  read interested message
            length = self._4bytes_to_int(buf[0:4])
            try: 
                assert length == 1
            except AssertionError:
                raise ProtocolError("Interested: bad length  received: {} expected: 1"\
                                        .format(length))
            peer.bt_state.interested = 1  # peer is interested in client
            self.logger.info('client successfully read {} from {} in state: {}'\
                 .format(bt_messages[ident], address, self.channel_state[address].state))
            # unchoke peer
            if peer.bt_state.choked and self.number_unchoked_peers < MAX_UNCHOKED_PEERS:
                self.send_unchoke_msg(peer)
                self.process_write_msg(peer, bt_messages_by_name['Unchoke'])
                self.number_unchoked_peers += 1
                self.logger.info('number of unchoked peers: {}'.format(self.number_unchoked_peers))
                peer.bt_state.choked = 0
                self.logger.info('sent unchoke msg to {}: number unchoked peers {}'\
                    .format(address, self.number_unchoked_peers))
            elif peer.bt_state.choked:
                # choke an unchoked peer to unchoke this one
                # or do nothing
                self.logger.info('number of unchoked peers is maxed out: '\
                    .format(MAX_UNCHOKED_PEERS))         
        elif ident == 3:
            #  read not interested message
            length = self._4bytes_to_int(buf[0:4])
            try: 
                assert length == 1
            except AssertionError:
                raise ProtocolError("Not Interested: bad length received: {} expected: 1"\
                                        .format(length))
            peer.bt_state.interested = 0  # peer is not interested in client
            self.logger.info('client successfully read {} from {} in state: {}'\
                 .format(bt_messages[ident], address, self.channel_state[address].state))
            # send peer Choke msg
            if peer.bt_state.choked == 0: # unchoked
                self.send_choke_msg(peer)
                self.process_write_msg(peer, bt_messages_by_name['Choke'])  # compare with process_server_write
                peer.bt_state.choked = 1
                self.number_unchoked_peers -= 1   
        elif ident == 4:
            # read have message:
            # peer.has_pieces.add(index)
            # piece_to_peers[index].add(address)
            # update piece_cnts[index] += 1
            self.logger.info('client successfully read {} from {} in state: {}'\
                 .format(bt_messages[ident], address, self.channel_state[address].state))
            msgd = self.rcv_have_msg(peer, buf)
            if self.channel_state[address].state == 2:
                self.channel_state[address].state = 3
            elif self.channel_state[address].state == 20:
                self.channel_state[address].state = 4
            elif self.channel_state[address].state == 21:
                self.channel_state[address].state = 50
            elif self.channel_state[address].state == 22:
                self.channel_state[address].state = 3
        elif ident == 5:
            #  bitfield message
            self.logger.info('client successfully read {} from {} in state: {}'\
                 .format(bt_messages[ident], address, self.channel_state[address].state))
            try:
                assert self.channel_state[address].state in [2, 20]
            except AssertionError:
                raise ProtocolError(
                    'Bitfield received in state {}. Did not follow Handshake.'\
                        .format(self.channel_state[address].state))
            try:
                msgd = self.rcv_bitfield_msg(peer, buf)  
            except AssertionError as e:
                self.logger.error('Bitfield has at least one 1 in \
                rightmost (padding) bits')
                raise ProtocolError from e
            self.channel_state[address].state = 4  # peer sends bitfield message
        elif ident == 6:
            # read request message
            self.logger.info('client successfully read {} from {} in state: {}'\
                    .format(bt_messages[ident], address, \
                    self.channel_state[address].state))
            # check request message
            try:
                msgd = self.check_request_msg(buf)
            except Exception as e:
                raise ProtocolError('check_request_msg: received bad request \
                from {}: {}'.format(address, e.args)) from e
             
            # peer requests block from client
            if peer.bt_state.interested and not peer.bt_state.choked:
                # send piece msg
                try:
                    yield from self.send_piece_msg(peer, msgd)
                except Exception as e:
                    raise ProtocolError('error in sending {} to {}'.format('Piece', address)) from e
                peer.num_bytes_downloaded += msgd['length']  # client sends block
                self.num_bytes_uploaded += msgd['length']
                self.process_write_msg(peer, bt_messages_by_name['Piece'])
            else:
                # ignore request
                pass
        elif ident == 7:
            #  read piece message
            try:
                result = self.rcv_piece_msg(msg)
            except BufferFullError as e:
                self.logger.debug('BufferFullError: {}'.format(address))
                raise e
            except Exception as e:
                self.logger.error('{}'.format(e.args))
                raise e
            self.logger.info('client successfully read {} from {} in state: {}'\
                 .format(bt_messages[ident], address, self.channel_state[address].state)) 
            if self.channel_state[address].state == 7:
                msg_length = self._4bytes_to_int(buf[0:4])
                if result == 'done':
                    self.channel_state[address].state = 8
                    peer.num_bytes_uploaded += msg_length - 9  # peer -> client
                    self.num_bytes_downloaded += msg_length - 9
                elif result == 'not done' or result == 'bad hash':
                    self.channel_state[address].state = 6
                    peer.num_bytes_uploaded += msg_length - 9  # length of block
                    self.num_bytes_downloaded += msg_length - 9
                elif result == 'already have block':
                    # piece msg is a duplicate: go to state 6
                    self.channel_state[address].state = 6
                    self.logger.info('duplicate block')
        elif ident == 8:
            #  cancel message
            pass # peer cancels block from piece
        elif ident == 9:
            #  port message
            pass
        elif ident == 20:
            # extension
            self.logger.info('extensions not supported')
            ident = NOTSUPPORTED_ID
        elif self.check_handshake_msg(peer, buf) == HANDSHAKE_ID:
                # received handshake msg
                self.logger.error('handshake msg unexpectedly received from {}'\
                    .format(address))
                raise ProtocolError('handshake msg unexpectedly received from {}'\
                    .format(address))
        else:
            # error
            self.logger.info("unknown bt message; received id: {}".format(ident))
            raise ProtocolError("unknown bt message; received id: {}".format(ident))
        # update peer time (last time self received a msg from peer)
        self.logger.info('client is in state {}'\
            .format(self.channel_state[address].state))
        peer.timer = datetime.datetime.utcnow()
 
        return ident

    def process_write_msg(self, peer, ident):
        """
        peer: peer object
        ident: integer identifying msg

        downloader:
        client writes:
        interested
        not interested
        request, cancel
        have, bitfield      

        uploader:
        client writes:
        choke
        unchoke
        have, bitfield
        piece
        """
        self.logger.info('in process_write_msg...')
        address = peer.address
        self.reset_keepalive(peer)
        if ident == 0:
            # client writes unchoke to peer
            peer.bt_state.choked = 0
        elif ident == 1:
            # client write choke to peer
            peer.bt_state.choke = 1  
        elif ident == 2:
            # client writes Interested to peer
            self.bt_state[address].interested = 1  # client interested in peer
            if self.channel_state[address].state == 4:
                self.channel_state[address].state = 5
            elif self.channel_state[address].state == 3:
                self.channel_state[address].state = 5
            elif self.channel_state[address].state == 50:
                self.channel_state[address].state = 6
            elif self.channel_state[address].state == 9:
                self.channel_state[address].state = 6
            elif self.channel_state[address].state == 10:
                self.channel_state[address].state = 5   # client writes Interested to peer
        elif ident == 3:
            # client writes Not Interested to peer
            self.bt_state[address].interested = 0  # client not interested in peer
            if self.channel_state[address].state == 8:
                self.channel_state[address].state = 9
            elif self.channel_state[address].state == 5:
                self.channel_state[address].state = 4 # changed from 3
            elif self.channel_state[address].state == 6: # added
                self.channel_state[address].state = 50
        elif ident == 4:
            # client writes Have to peer
            pass # client writes Have to peer
        elif ident == 6:
            # client writes Request to peer
            if self.channel_state[address].state == 6:
                self.channel_state[address].state = 7
            elif self.channel_state[address].state == 8:
                self.channel_state[address].state = 7 # client writes Request for block
        elif ident == 7:
            # client writes Piece msg to peer
            pass # client writes Piece to peer
        elif ident == 8:
            # client writes Cancel to peer
            if self.channel_state[address].state == 7:
                self.channel_state[address] = 6
            elif self.channel_state[address] in [8, 9, 10]:
                pass
            elif self.channel_state[address] < 7 or self.channel_state[address] == 30:
                raise ProtocolError("Cancel received in an invalid state {}".format(self.channel_state[address].state)) # client writes Cancel to peer
        else:
            self.logger.info("Client wrote Unknown message ident: {}".format(ident))
            raise ProtocolError("Client wrote Unknown message ident: {}".format(ident))
        peer._client_keepalive_timer = datetime.datetime.utcnow()
        self.logger.info('client successfully wrote {} to {}'\
            .format(bt_messages[ident], peer.address))

    @asyncio.coroutine
    def handle_leecher(self, reader, writer):
        """
        listen for leechers
        leecher initiates connection

        read Handshake, write Handshake+bitfield
        """
        self.logger.info('in handle_leecher...')

        # get peer address
        address = writer.get_extra_info('peername')
        peer = Peer(self.torrent, address)
        peer.reader = reader
        peer.writer = writer

        self.logger.info('in handle_leecher peername: {}'.format(address))

        try:
            yield from self._open_leecher_connection(peer) # sets server_channel_state
        except (ProtocolError, Exception) as e:
            self.logger.error('in handle_leecher: error in opening connection to peer {}: {}' \
            .format(address, e.args))
            # close reader, writer
            peer.reader.set_exception(None)
            peer.writer.close()
            self.closed_leechers_cnt += 1
            peer = None
            return

        # add peer joining swarm to active_peers
        self.active_peers[address] = peer
        self.channel_state[address] = ChannelState(open=1)
        self.channel_state[address].state = self.server_channel_state[address]
        self.bt_state[address] = BTState(interested=0, choked=1)

        while True:
            # read next msg
            try:
                yield from self.read_peer(peer)
            except (ProtocolError, Exception) as e:
                self.logger.error(e.args)
                self._close_peer_connection(peer)
                return

    def close_quiet_connections(self, peer):
        """
        close a connection that has timed out
        timeout is CONNECTION_TIMEOUT secs
        """
        if datetime.datetime.utcnow() - peer.timer > CONNECTION_TIMEOUT:
            # close connection
            self.logger.info('closing quiet connection to {}'.format(peer.address))
            self._close_peer_connection(peer)
    
    @asyncio.coroutine            
    def send_keepalive(self, peer):
        """sends keep_alive to peer if timer was updated > CONNECTION_TIMEOUT secs ago"""
        if datetime.datetime.utcnow() - peer._client_keepalive_timer > CONNECTION_TIMEOUT:
            peer.writer.write(KEEPALIVE)
            yield from peer.writer.drain()

            peer._client_keepalive_timer = datetime.datetime.utcnow()
            self.logger.info('wrote KEEPALIVE to {}'.format(peer.address))
          
    @asyncio.coroutine
    def connect_to_peer(self, peer):
        """connect to peer address (ipv4, port) 
        
        write handshake to peer
        read handshake from peer"""

        address = peer.address
        ip, port = address

        self.logger.info('in connect_to_peer: {}'.format(address))
        try:
            reader, writer = yield from asyncio.open_connection(host=ip, port=port)
        except (TimeoutError, OSError) as e:
            self.logger.info("connect_to_peer {}: {}".format(address, e.args))
            self.closed_ips_cnt += 1
            del self.active_peers[address]
            del self.channel_state[address]
            del self.bt_state[address]
            self.num_peers = len(self.active_peers)
            return
        except Exception as e:
            self.logger.error("connect_to_peer: {} Other Exception..{}: {}".format(adddress, e.args))
            self.closed_ips_cnt += 1
            del self.active_peers[address]
            del self.channel_state[address]
            del self.bt_state[address]
            self.num_peers = len(self.active_peers)
            return

        # successful connection to peer
        # attach reader, writer to peer
        peer.reader = reader
        peer.writer = writer
        # write Handshake; read Handshake
        try:
            yield from self._open_peer_connection(peer)
        except ProtocolError as e:
            self.logger.error("handshake failed; closing connection to {}".format(address, e.args))
            self._close_peer_connection(peer)

        self.logger.info('{}: connection open'.format(address))       
            
        while not peer.has_pieces:
            # no bitfield or have msgs yet
            self.logger.info('{}: while loop: (no bitfield/have)'.format(address))

            try:    
                msg_ident = yield from self.read_peer(peer)

            except (ProtocolError, TimeoutError, OSError) as e:
                self.logger.error("connect_to_peer: {}: reading bitfield/have {}".format(address, e.args))
                self._close_peer_connection(peer)
            except Exception as e:
                self.logger.error('connect_to_peer:  {}: reading bitfield/have {}'.format(address, e.args))
                self._close_peer_connection(peer)
            
            self.logger.info("connect_to_peer: {}: successfully read {}".format(address, bt_messages[msg_ident]))
        self.logger.info('connected to {}. It has pieces {}'.format(address, peer.has_pieces))

    @asyncio.coroutine
    def wait_for_peers_to_have_pieces(self):
        """
        client waits for Have msgs from its peers
        reads all peers; if it has 1 or more Have msgs, method returns
        """
        self.logger.info('in wait_for_peers_to_have_pieces...')

        while not self.piece_cnts:
            # peers have no pieces client wants

            try:
                peers = [peer for address, peer in self.active_peers.items() if self.channel_state[address].open]   
                for peer in peers:
                    # side effect: Have msg updates:
                    # self.piece_cnt
                    # peer.has_pieces
                    msg_ident = yield from self.read_peer(peer) 
                    if bt_messages[msg_ident] == 'Have':
                        break
            except (ProtocolError, TimeoutError, OSError, Exception) as e:
                self.logger.error("waiting_for_peers_to_have_pieces: waiting for Have; \
                error while reading: {}".format(e.args))
                self._close_peer_connection(peer)
                 
    def select_piece(self):
        # start the piece process
        # interested --> request --> process blocks received --> request -->...
        #
        # select a piece_index and a peer
        self.logger.info('in select piece...')
        try:
            result = self._get_next_piece()
        except ConnectionError as e:
            self.logger.error('select_piece: error in get_next_piece {}'.format(e.args))
            raise e
        except Exception as e:
            self.logger.error('select_piece: error in get_next_piece {}'.format(e.args))
            raise e
        if result == 'success':
            # [rindex, {set of rpeers}] assigned to self.selected_index_peers
            # {rindex: {set of rpeers}} -- new
            return result
        elif result == 'no open connections':
            self.logger.info('no open connections')
            return result # connect to tracker
        elif result == 'need more pieces': # self.piece_cnts has been consumed
            # this should not happen
            self.logger.info('self.piece_cnts have been consumed')
            return result # connect to tracker

    @asyncio.coroutine 
    def get_piece(self, rindex, rpeer):
        """
        run through protocol with rpeer
        repeat: write request/read piece - until entire piece is downloaded
        """
        self.logger.info('in get_piece...')
        address = rpeer.address

        while self.channel_state[address].open and rindex not in self.pbuffer.completed_pieces:
            # if not interested: write Interested
            if self.channel_state[address].open and not self.bt_state[address].interested:
                
                # write Interested to peer
                try:
                    rpeer.writer.write(INTERESTED)
                    yield from rpeer.writer.drain()
                except Exception as e:
                    self.logger.error('{}: error in writing Interested'\
                        .format(address, e.args))
                    self._close_peer_connection(rpeer)
                finally:
                    if not self.active_peers:
                        # no open connections
                        return

                self.process_write_msg(rpeer, bt_messages_by_name['Interested'])
                
                self.logger.info("{}: wrote INTERESTED state {}".\
                    format(address, self.channel_state[address].state))
            else:
                # channel is closed or client is already interested
                pass
        
            # if Choked, Read until Unchoked
            while self.channel_state[address].open and self.bt_state[address].choked:

                self.logger.info("{}: client ready to receive Unchoke state {}"\
                    .format(address, self.channel_state[address].state))
                try:
                    msg_ident = yield from self.read_peer(rpeer)  
                except (ProtocolError, TimeoutError, OSError, ConnectionResetError) as e:
                    self.logger.error('get_piece: {} expected Unchoke {}'.\
                        format(address, e.args))
                    self._close_peer_connection(rpeer)
                except Exception as e:
                    self.logger.error('get_piece: {} expected Unchoke Other Exception 2 {}'\
                        .format(address, e.args))
                    self._close_peer_connection(rpeer)
                finally:
                    if not self.active_peers:
                        # close Task so loop stops and program can reconnect to Tracker 
                        return
                self.logger.info("{}: received {} state {}"\
                    .format(address, bt_messages[msg_ident], self.channel_state[address].state))
                if bt_messages[msg_ident] == 'Unchoke':
                    break

            # channel is closed or already unchoked
                 
            # write Request and read Piece
            if self.channel_state[address].open and \
                not self.bt_state[address].choked and \
                rindex not in self.pbuffer.completed_pieces:
                    if not self.pbuffer.is_registered(rindex):
                        try:
                            self.pbuffer._register_piece(rindex)
                        except BufferFullError as e:
                            self.write_buffer_to_file()
                            self.pbuffer._register_piece(rindex)
                        except Exception as e:
                            self.logger.error(e.args)
                        self.logger.info("get_piece: {} not in buffer. Registering piece {}... state {}"\
                            .format(rindex, rindex, self.channel_state[address].state))

                    # write Request
                    offset = self.pbuffer.piece_info[rindex]['offset']
                    # update offset; invariant: offset stores value for next request
                    # rindex must be registered in piece_info first
                    self.pbuffer.piece_info[rindex]['offset'] = self._new_offset(rindex, offset)

                    self.logger.info('get_piece: index {} ready to write Request to {} begin: {} state {}'\
                        .format(rindex, address, offset, self.channel_state[address].state))
                    
                    # construct request msg
                    msg = self.make_request_msg(rindex, offset)
                    
                    try:
                        rpeer.writer.write(msg)
                        yield from rpeer.writer.drain()
                    except Exception as e:
                        self.logger.error('get_piece: {} write Request'.format(address, e.args))
                        self._close_peer_connection(rpeer)
                    finally:
                        if not self.active_peers:
                            # no open connections
                            return
                    
                    self.process_write_msg(rpeer, bt_messages_by_name['Request'])
                    self.logger.info("get_piece: {}: wrote Request for index {} begin {} state {}"\
                        .format(address, rindex, offset, self.channel_state[address].state))
                    self.logger.info('get_piece: {} expect to receive Piece {} state {}'\
                        .format(address, rindex, self.channel_state[address].state))
                
                    # read until Piece
                    try:
                        msg_ident = yield from self.read_peer(rpeer)

                    except (ProtocolError, TimeoutError, OSError) as e:
                        self.logger.error('get_piece: {} expected Piece {}'.format(address, e.args))
                        self._close_peer_connection(rpeer)
                    except Exception as e:
                        self.logger.error("get_piece: expect to read Piece from: {} {} \
                        channel_state: {} \
                        open: {} \
                        choked: {} \
                        interested: {}"\
                            .format(e.args, address, \
                            self.channel_state[address].state, \
                            self.channel_state[address].open,\
                            self.bt_state[address].choked, \
                            self.bt_state[address].interested))
                        self._close_peer_connection(rpeer)
                    finally:
                        if not self.active_peers:
                            # no open connections
                            return

                    self.logger.info("get_piece: {}: successfully read {} index: {} state {}"\
                        .format(address, bt_messages[msg_ident], rindex, self.channel_state[address].state))

                    while bt_messages[msg_ident] != 'Piece' and \
                        self.channel_state[address].open and \
                        not self.bt_state[address].choked:
                        try:
                            msg_ident = yield from self.read_peer(rpeer)

                        except (ProtocolError, TimeoutError, OSError) as e:
                            self.logger.error('downloader: {} reading for Piece {}'.format(address, e.args))
                            self._close_peer_connection(rpeer)
                        except Exception as e:
                            self.logger.error("downloader: reading for Piece: {} \
                            channel_state: {} \
                            open: {} \
                            choked: {} \
                            interested: {}"\
                                .format(address, \
                                self.channel_state[address].state, \
                                self.channel_state[address].open,\
                                self.bt_state[address].choked, \
                                self.bt_state[address].interested))
                            self._close_peer_connection(rpeer)
                        finally:
                            if not self.active_peers:
                                # no open connections
                                return
                        self.logger.info("get_piece: {}: successfully read {} state {}"\
                            .format(address, bt_messages[msg_ident], self.channel_state[address].state))
                    # received Piece msg or Choke msg or Exception \
                    # (if exception: client closes connection to rip)
                    # top of while loop: if not all_pieces(), get a piece index 
                    # (could be the same piece index but with a new offset)
            else:
                # channel is closed, choked, or all pieces from peers are complete
                pass
        return
                             
    @asyncio.coroutine
    def read_peer(self, peer):
        """
        read msg from peer
        call process_read_msg to process it

        throws ProtocolError, Exception
        """
        address = peer.address

        self.logger.info('in read_peer')
        
        try:
            msg_length = yield from peer.reader.readexactly(4)

        except (TimeoutError, OSError) as e:
            self.logger.error('caught error from reading msg_length: {}'.format(address, e.args))
            raise ProtocolError from e
        except Exception as e:
            self.logger.error('caught Other Exception 2:  {}'.format(address, e.args))
            raise ProtocolError from e
        if msg_length == KEEPALIVE:
            peer.timer = datetime.datetime.utcnow()
            self.logger.info('received Keep-Alive from peer {}'.format(address))
        else:
            try:
                msg_body = yield from peer.reader.readexactly(self._4bytes_to_int(msg_length))

            except (TimeoutError, OSError) as e:
                self.logger.error("{}: caught error from body {}"\
                    .format(address, e.args))
                raise protocolError from e
            except Exception as e:
                self.logger.error("{}: caught Other Exception {}"\
                    .format(address, e.args))
                raise ProtocolError from e
            msg_ident = msg_body[0]
            try:
                # processing the read msg happens here
                yield from self.process_read_msg(peer, msg_length + msg_body)
            except (ProtocolError, Exception) as e:
                self.logger.error('read_peer: error in process_read_msg from {}: {}'.format(address, e.args))
                raise ProtocolError from e
                
        return msg_ident 