"""
Leslie B. Klein
Dec. 1, 2015

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
from bt_utils import MAX_PEERS_TO_REQUEST_FROM  # used in client._get_next_piece()
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
        self.leecher_state = 0

        # peer keepalive timer (reset when client receives msg from peer)
        self.timer = datetime.datetime.utcnow()
         
        # client keepalive timer (reset when client sends msg to peer)
        self._client_keepalive_timer = datetime.datetime.utcnow()

        # statistics
        self.number_bytes_uploaded = 0     # client.process_read_msg (client reads piece msg)
        self.number_bytes_downloaded = 0   # client.process_read_msg (client sends piece msg)
        
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

        self.active_peers = {}  # updated with each tracker response; open and not-yet-tried-to-open peers
        self.bt_state = {}      # {ip: BTState()}  choked=1 interested=0
        self.channel_state = {} # {ip: ChannelState()} open=0 state=0

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
        #self.selected_piece_peer = [] # [piece_index, peer] NOT USED since refactoring
        self.selected_piece_peers = [] # [piece_index, <set of peers>]

        # used when uploading (server)
        self.number_unchoked_peers = 0
        # used by server (listens for incoming (leecher) connections)
        self.leecher_conn = {} # {ip: peer} # leechers initiate connection to client
        # store server state:
        # choked/not-choked by leecher
        # interested/not-interested in leecher
        self.server_bt_state = {} # {ip: BTState(choked=0, interested=0)}
        self.closed_leechers_cnt = 0


        # used to read files from disk to cache to pbuffer
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
            #print('shutdown: flushing buffer...')

            self.write_buffer_to_file()

            self.logger.info('shutdown: flushed buffer')
            #print('shutdown: flushed buffer')

        # close all open files
        if not self.seeder and self.output_fds: # leecher has opened file(s)
            self._close_fds()
            #print('closed all open files...')
            self.logger.info('closed all open files...')

        # shutdown client
        #print('client is shutting down...')
        self.logger.info('client is shutting down...')

        success = all([self._close_peer_connection(peer) \
            for ip, peer in self.open_peers().items()]) and \
            all([self._close_leecher_connection(peer) \
            for ip, peer in self.open_leechers().items()])

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
            #print('shutdown: client did not close all open peers/leechers')
            self.logger.info('shutdown: client did not close all open peers/leechers')
            return False

    def _parse_active_peers_for_testing(self, address_list):
        """
        use this with a real peer instead of connecting to tracker
        """
        ptorrent = self.torrent
        for address in address_list:
            ip, port = address
            self.active_peers[ip] = Peer(ptorrent, address)
            self.channel_state[ip] = ChannelState()
            self.bt_state[ip] = BTState()
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
        self.active_peers: {ip: Peer(self.torrent, (ip, port)), ...}
        self.bt_state: {ip: BTState() for ip in dict_peers}
        self.channel_state: {ip: ChannelState() for ip in dict_peers}
        """
        peers = self.tracker_response['peers']
        dict_of_peers = {ip: Peer(self.torrent, (ip, port_bytes[0]*256 + port_bytes[1]))
                for index in range(len(peers)//6)
                for port_bytes in [peers[(index + 1)*4:(index + 1)*4 + 2]]
                for ip in [socket.inet_ntoa(peers[index*4:(index + 1)*4])]}
        self.bt_state.update({ip: BTState() \
            for ip in dict_of_peers if ip not in self.bt_state})
        self.channel_state.update({ip: ChannelState() \
            for ip in dict_of_peers if ip not in self.channel_state})
        # after a peer channel transitions from open to close, it is removed from active_peers
        # initially, a peer is in active_peers with channel closed
        self.active_peers.update({ip: dict_of_peers[ip] \
            for ip in dict_of_peers if ip not in self.active_peers})
        self.num_peers = len(self.active_peers)  # number of open and not-yet-connected-to peers

    def open_peers(self):
        """
        returns dict of all open peer connections
        """
        if not self.active_peers:
            return {}
        else:
            return {ip:peer for ip, peer in self.active_peers.items() if self.channel_state[ip].open}

    def open_leechers(self):
        """
        returns dict of all open leecher connections
        """
        if not self.leecher_conn:
            return {}
        else:
            return {ip:peer for ip, peer in self.leecher_conn.items()}

    def reset_keepalive(self, peer):
        # timer stores the last time client wrote to peer
        peer._client_keepalive_timer = datetime.datetime.utcnow()

    def _close_leecher_connection(self, peer):
        """clean-up after failed connection to leecher"""
        ip, port = peer.address
        # close leecher connection
        del self.leecher_conn[ip]
        # close reader, writer
        peer.reader.set_exception(None)
        peer.writer.close()
        self.closed_leechers_cnt += 1

        self.logger.info('{}: cleaned up after closing connection'.format(peer.address))
        #print('{}: cleaned up after closing connection'.format(peer.address))
        return True

    def _close_peer_connection(self, peer):
        """clean-up after failed connection to peer"""
        ip, _ = peer.address
        # close channel state
        del self.channel_state[ip]
        del self.bt_state[ip]
        del self.active_peers[ip]
        self.num_peers = len(self.active_peers)
        # close reader, writer
        peer.reader.set_exception(None)
        peer.writer.write(b'')
        peer.writer.close()
        # incr number of closed connections
        self.closed_ips_cnt += 1
        self.logger.info('{}: cleaned up after closing connection'.format(ip))
        #print('{}: cleaned up after closing connection'.format(ip))
        return True

    def _open_peer_connection(self, peer, reader, writer):
        """
        sets channel_state[ip]
        attaches reader, writer to peer object
        """
        self.logger.info('_open_peer_connection to {}'.format(peer.address[0]))
        #print('_open_peer_connection to {}'.format(peer.address[0]))       

        ip, _ = peer.address
        peer.reader = reader
        peer.writer = writer
        peer.writer.write(self.HANDSHAKE)
        self.channel_state[ip].open = 1
        self.channel_state[ip].state = 1
        self.logger.info('write Handshake to {}'.format(ip))
        #print('write Handshake to {}'.format(ip))
        #peer.writer.write(self.HANDSHAKE)
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
        #fds = []
        #for file in self.files:
        #    try:
        #        f = open(file['name'], mode='wb')
        #    except Exception as e:
        #        f = open(file['name'], mode='xb')
        #    fds += [f]
        return fds

    def write_buffer_to_file(self, reset_buffer=False):
        """
        write all pieces in buffer to the filesystem using the pathname
        """
        # open files
        self.output_fds = self.open_files()   # moved from init

        # write entire buffer to file(s)
        try:
            for piece_index in self.pbuffer.piece_info.keys():
                self._write_piece_to_file(piece_index)
        except Exception as e:
            print('write_buffer_to_file: {}'.format(e.args))
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
                self.logger.info('about to write file {}: offset {} start {} nbytes {} row {}'\
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

    def _get_next_piece(self, num_peers=MAX_PEERS_TO_REQUEST_FROM):
        """
        yield [piece_index, {set of ips})

        1. find connected peers (self.channel_state[ip].open == 1)
        2. peer.has_pieces is a set of piece indices
        3. get most rare piece that client doesn't already have
        """
        most_common = self.piece_cnts.most_common()
        # [(index, cnt), ..., (index, cnt)]

        if most_common:
            if len(most_common) <= num_peers:
                list_of_indices = most_common[:]
            else:
                list_of_indices = most_common[-1:-num_peers-1:-1]

            # get all peers (ips) with rarest piece
            index, cnt = list_of_indices[-1]
            ips_with_least = list(set(self.channel_state.keys()).intersection(self.piece_to_peers[index]))
            if ips_with_least:
                self.selected_piece_peers = [index, {self.active_peers[ip] for ip in ips_with_least}]
                #print('_get_next_piece: index {} peers {}'.format(index, self.selected_piece_peers[1]))
                self.logger.info('_get_next_piece: index {} peers {}'.format(index, self.selected_piece_peers[1]))
                return 'success'
            else:
                # no open connections with pieces (all open connections have shut down)
                return 'no open connections'
        else:
            # no pieces left in self.piece_cnts
            self.logger.info('_get_next_piece: no pieces left in piece_cnts')
            self.logger.info('_get_next_piece: {} piece_cnts: {}  \
            most_common: {}'.format(ip, self.piece_cnts, most_common))
            #print('_get_next_piece: {} piece_cnts: {}  \
            #most_common: {}'.format(ip, self.piece_cnts, most_common))
            return 'need more pieces'

    def all_pieces(self):
        return len(self.pbuffer.completed_pieces) == self.torrent.number_pieces

    def _filter_on_not_complete():
            return {pindex: ips for pindex, ips in self.piece_to_peers.items() 
                    if pindex not in self.pbuffer.completed_pieces}

    def rcv_have_msg(self, ip, buf):
        """
        update client's representation of the sending peer's bitfield
        peer's bitfield indicates the pieces the peer has 
        """
        # buf = bytearray(msg)
        msgd = {}
        msgd['length'] = self._4bytes_to_int(buf[:4])
        msgd['id'] = buf[4]
        msgd['piece index'] = self._4bytes_to_int(buf[5:9])
        index = msgd['piece index']
        # update data structures
        self.active_peers[ip].has_pieces.add(index)
        self.piece_to_peers[index].add(ip)
        if index not in self.pbuffer.completed_pieces:
            self.piece_cnts[index] += 1
        return msgd

    def rcv_bitfield_msg(self, ip, buf):
        """
        rcv bitfield from peer
        set client's representation of peer's bitfield
        """
        #buf = bytearray(msg)
        msgd = {}
        msgd['length'] = self._4bytes_to_int(buf[:4])
        msgd['id'] = buf[4]
        msgd['bitfield'] = buf[5:]
        self.active_peers[ip].bitfield = msgd['bitfield']
        self.active_peers[ip].has_pieces = self.get_indices(msgd['bitfield'])
        # update data structures
        for index in self.active_peers[ip].has_pieces:
            self.piece_to_peers[index].add(ip)
            if index not in self.pbuffer.completed_pieces:
                self.piece_cnts[index] += 1
        return msgd

    def _parse_request_msg(self, buf):
        """
        parse request msg

        make piece msg to sender
        """
        #buf = bytearray(msg)
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
        #buf = bytearray(msg)
        msgd = self._parse_piece_msg(buf)
        length = msgd['length']
        ident = msgd['ident']
        index = msgd['index']
        begin = msgd['begin']
        block = msgd['block']

        if self.is_block_received(index, begin):
            #print('rcv_piece_msg: received duplicate block index {} begin: {}'\
            #    .format(index, begin))
            self.logger.info('rcv_piece_msg: received duplicate block index {} begin: {}'\
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
            #print('rcv_piece_msg: bad hash index: {}'.format(index))
            logging.debug('rcv_piece_msg: bad hash index: {}'.format(index))
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
        for ip, peer in self.open_peers().items():
            if self.bt_state[ip].interested:
                self.bt_state[ip].interested = 0
                try:
                    peer.writer.write(NOT_INTERESTED)
                except Exception as e:
                    #print('error in sending Not Interested to {} {}'.format(ip, e.args))
                    self.logger.error('error in sending Not Interested to {} {}'.format(ip, e.args))
                #print('send_not_interested_to_all: {}'.format(ip))
                self.logger.info('send_not_interested_to_all: {}'.format(ip))


    @asyncio.coroutine
    def send_piece_msg(self, peer, msgd):
        """
        buf: bytearray(request msg)
        constructs Piece msg from Request msg
        writes Piece msg to peer
        return Piece msg
        """
        index = msgd['index']
        begin = msgd['begin']
        block_length = msgd['block_length']
        msg = self.make_piece_msg(index, begin, block_length)
        try:
            peer.writer.write(msg)
            yield from peer.writer.drain()
        except Exception as e:
            #print('send_piece_msg: error in writing msg to {} index {} begin {}'\
            #    .format(peer.address[0], index, begin))
            self.logger.error('send_piece_msg: error in writing msg to {} index {} begin {}'\
                .format(peer.address[0], index, begin))
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
            #print('block has never been received or even registered! index {} block {} offset {}'\
            #    .format(index, block_num, begin))
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
        msg = self.make_have_msg(piece_index)
        for ipx, peerx in self.open_peers().items():
            if not peerx.has_piece(piece_index):
                try:
                    peerx.writer.write(msg)
                except Exception as e:
                    print(e.args)
                    logging.debug('rcv_piece_msg::done error in writing Have to {}'.format(ipx))
                    print('rcv_piece_msg::done error in writing Have to {}'.format(ipx))
                    raise e
                print('rcv_piece_msg::done successfully wrote Have msg to {}'.format(ipx))
                logging.debug('rcv_piece_msg::done successfully wrote Have msg to {}'.format(ipx))
            else:
                # peer already has piece
                pass

    def send_choke_msg(self, peer):
        try:
            peer.writer.write(CHOKE)
        except Exception as e:
            print('send_choke_msg: {}'.format(e.args))
            logging.debug('send_choke_msg: {}'.format(e.args))
            raise e

    def send_unchoke_msg(self, peer):
        try:
            peer.writer.write(UNCHOKE)
        except Exception as e:
            print('send_unchoke_msg: {}'.format(e.args))
            logging.debug('send_unchoke_msg: {}'.format(e.args))
            raise e

    def check_handshake_msg(self, peer, buf):
        """
        buf: bytearray(msg)
        """
        ip, _ = peer.address
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
                print("check_handshake_msg: received handshake\
                msg with length {}; should be 19".format(buf[0].decode()))
                raise ProtocolError("check_handshake_msg: \
                received handshake msg with length {}; should be 19"\
                    .format(buf[0].decode()))
            #  handshake message
            msgd = self._parse_handshake_msg(buf)
            try:
                # check info_hash
                assert msgd['info_hash'] == self.torrent.INFO_HASH
            except AssertionError as e:
                print("check_handshake_msg: peer is not part of torrent:\
                expected hash: {}".format(self.torrent.INFO_HASH))
                raise ProtocolError("check_handshake_msg: \
                peer is not part of torrent: expected hash: {}"\
                    .format(self.torrent.INFO_HASH))

            # Note: check peer_id
            # check that peer.peer_id from tracker_response['peers'] (dictionary model)
            # matches peer.peer_id in handshake (msgd['peer_id'])
            # tracker response uses compact mode so peer.peer_id is not in tracker_response

            # set peer_id of peer
            peer.peer_id = msgd['peer_id']
            # reset peer timer
            peer.timer = datetime.datetime.utcnow()

            ident = HANDSHAKE_ID  # identifies handshake
            return ident

    def check_request_msg(self, msgd):
        """
        msgd: dictionary containing values in msg
        """
        length = msgd['length']
        ident = msgd['id']
        index = msgd['index']
        begin = msgd['begin']
        block_length = msgd['block_length']
        # check if begin + block_length > piece_length
        if index == self.torrent.LAST_PIECE_INDEX:
            # last piece
            piece_length = self.last_piece_length
        else:
            piece_length = self.piece_length
        try:
            assert begin + block_length <= piece_length
        except AssertionError as e:
            print('check_request_msg: {}'.format(e.args))
            logging.debug('check_request_msg: {}'.format(e.args))
            raise ProtocolError('check_request_msg: begin + block_length \
            exceeds piece length index {}'.format(index))
        except Exception as e:
            print(e.args)

    def check_bitfield_msg(self, msgd):
        """
        msgd: dictionary containing values in msg
        """
        bitfield = msgd['bitfield']  # bytearray
        number_padding_bits = 8 - self.torrent.number_pieces % 8
        try:
            assert bitfield[-1] >> number_padding_bits << number_padding_bits == bitfield[-1]
        except AssertionError as e:
            logging.debug('check_bitfield_msg: bitfield padding bits contain at least one 1 index {}'\
                .format(msgd['index']))
            print('check_bitfield_msg: bitfield padding bits contain at least one 1 index {}'\
                .format(msgd['index']))
            raise e

    @asyncio.coroutine
    def process_read_msg(self, peer, msg):
        """process incoming msg from peer - protocol state machine
        
        peer: peer instance
        msg: bittorrent msg
        """
        if not msg:
            return

        buf = bytearray(msg)
        ip, port = peer.address
        
        # check for handshake
        try:
            ident = self.check_handshake_msg(peer, buf)
        except (ConnectionError, ProtocolError) as e:
            print('process_read_msg: received Handshake msg with errors')
            raise e
        if ident == HANDSHAKE_ID: # could be False or HANDSHAKE_ID
            if self.channel_state[ip].state == 1:
                self.channel_state[ip].state = 2
            return ident

        ident = buf[4]

        if ident == 0:
            #  choke message
            length = self._4bytes_to_int(buf[0:4])
            try:
                assert length == 1
            except AssertionError:
                raise ConnectionError("Choke: bad length  received: {} expected: 1"\
                                        .format(length))
            peer.timer = datetime.datetime.utcnow()
            self.bt_state[ip].choked = 1  # peer chokes client
            if self.channel_state[ip].state == 30:
                self.channel_state[ip].state = 3
            elif self.channel_state[ip].state == 6:
                self.channel_state[ip].state = 5
            elif self.channel_state[ip].state == 7:
                self.channel_state[ip].state = 71
            elif self.channel_state[ip].state == 9:
                self.channel_state[ip].state = 10
            elif self.channel_state[ip].state == 50:
                self.channel_state[ip].state = 4
            elif self.channel_state[ip].state == 8:
                self.channel_state[ip].state = 81
            else:
                pass   # peer chokes client

        elif ident == 1:
            #  unchoke message
            length = self._4bytes_to_int(buf[0:4])
            try: 
                assert length == 1
            except AssertionError:
                raise ConnectionError("Unchoke: bad length  received: {} expected: 1"\
                                        .format(length))
            peer.timer = datetime.datetime.utcnow()
            self.bt_state[ip].choked = 0  # peer unchoked client
            if self.channel_state[ip].state == 3:
                self.channel_state[ip].state = 30
            elif self.channel_state[ip].state == 5:
                self.channel_state[ip].state = 6
            elif self.channel_state[ip].state == 10:
                self.channel_state[ip].state = 9
            elif self.channel_state[ip].state == 4:
                self.channel_state[ip].state = 50
            elif self.channel_state[ip].state == 71:
                # when waiting to recv Piece but get Unchoke, go back to Req
                self.channel_state[ip].state = 6  
            elif self.channel_state[ip].state == 81:
                self.channel_state[ip].state = 8
            else:
                pass # peer unchokes client
        elif ident == 2:
            #  interested message
            length = self._4bytes_to_int(buf[0:4])
            try: 
                assert length == 1
            except AssertionError:
                raise ConnectionError("Interested: bad length  received: {} expected: 1"\
                                        .format(length))
            peer.timer = datetime.datetime.utcnow()
            peer.bt_state.interested = 1  # peer is interested in client
            # unchoke peer
            if self.number_unchoked_peers < MAX_UNCHOKED_PEERS:
                self.send_unchoke_msg(peer)
                self.number_unchoked_peers += 1
                self.process_write_msg(peer, ident)
                print('process_read_msg: sent unchoke msg to {} number unchoked peers {}'\
                    .format(ip, self.number_unchoked_peers))
                logging.debug('process_read_msg: sent unchoke msg to {} number unchoked peers {}'\
                    .format(ip, self.number_unchoked_peers))
            else:
                # choke an unchoked peer to unchoke this one
                # or do nothing
                print('process_read_msg: number of unchoked peers is {}'\
                    .format(MAX_UNCHOKED_PEERS))
                logging.debug('process_read_msg: number of unchoked peers is {}'\
                    .format(MAX_UNCHOKED_PEERS)) # peer sends Interested msg
                
        elif ident == 3:
            #  not interested message
            length = self._4bytes_to_int(buf[0:4])
            try: 
                assert length == 1
            except AssertionError:
                raise ConnectionError("Not Interested: bad length  received: {} expected: 1"\
                                        .format(length))
            peer.timer = datetime.datetime.utcnow()
            peer.bt_state.interested = 0  # peer is not interested in client
            # send peer Choke msg
            if peer.bt_state.choked == 0: # unchoked
                self.send_choke_msg(peer)
                self.number_unchoked_peers -= 1
                self.process_write_msg(peer, ident)

        elif ident == 4:
            #  have message
            try:
                assert self.channel_state[ip].state != 1
            except AssertionError:
                raise ProtocolError("cannot receive Have msg in state 1")
            peer.timer = datetime.datetime.utcnow()
            msgd = self.rcv_have_msg(ip, buf)
            if self.channel_state[ip].state == 4:
                self.channel_state[ip].state = 3  # peer sends have message 
        elif ident == 5:
            #  bitfield message
            try:
                assert self.channel_state[ip].state == 2
            except AssertionError:
                raise ProtocolError(
                    'Bitfield received in state {}. Did not follow Handshake.'\
                        .format(self.channel_state[ip].state))
            peer.timer = datetime.datetime.utcnow()
            msgd = self.rcv_bitfield_msg(ip, buf)  # set peer's bitfield
            try:
                self.check_bitfield_msg(msgd)
            except AssertionError:
                raise ProtocolError(
                    'Bitfield has at least one 1 in rightmost {} (padding) bits'\
                        .format(number_padding_bits))
            self.channel_state[ip].state = 4  # peer sends bitfield message
        elif ident == 6:
            #  request message
            peer.timer = datetime.datetime.utcnow()
            msgd = self._parse_request_msg(buf)
            # check request message
            try:
                self.check_request_msg(msgd)
            except Exception as e:
                raise ProtocolError('check_request_msg: {}'.format(e.args))
             
            # peer requests block from client
            if peer.bt_state.interested and not peer.bt_state.choked:
                # send piece msg
                try:
                    yield from self.send_piece_msg(peer, msgd)
                except Exception as e:
                    self._close_peer_connection(peer)
                peer.number_bytes_downloaded += msgd['length']  # client sends block
                self.number_bytes_uploaded += msgd['length']
                self.process_write_msg(peer, ident)  # peer sends Request msg; client sends Piece msg
        elif ident == 7:
            #  piece message
            peer.timer = datetime.datetime.utcnow()
            try:
                result = self.rcv_piece_msg(msg)
            except BufferFullError as e:
                logging.debug('process_read_msg: BufferFullError ip: {}'.format(ip))
                print('process_read_msg: BufferFullError ip: {}'.format(ip))
                raise e
            except Exception as e:
                print('process_read_msg: {}'.format(e.args))
                logging.debug('process_read_msg: {}'.format(e.args))
                raise e

            msgd = self._parse_piece_msg(buf)
            if self.channel_state[ip].state == 7:
                if result == 'done':
                    self.channel_state[ip].state = 8
                    peer.number_bytes_uploaded += msgd['length'] - 9  # peer -> client
                elif result == 'not done' or result == 'bad hash':
                    self.channel_state[ip].state = 6
                    peer.number_bytes_uploaded += msgd['length'] - 9  # length of block
                elif result == 'already have block':
                    # piece msg is a duplicate: go to state 6
                    self.channel_state[ip].state = 6
                    print('process_read_msg: duplicate block')
                    logging.debug('process_read_msg: duplicate block')
            pass  # peer sends block to client
        elif ident == 8:
            #  cancel message
            peer.timer = datetime.datetime.utcnow()
            pass # peer cancels block from piece
        elif ident == 9:
            #  port message
            peer.timer = datetime.datetime.utcnow()
            pass # peer sends port message
        elif ident == 20:
            # extension
            peer.timer = datetime.datetime.utcnow()
            print('extensions not supported')
            ident = NOTSUPPORTED_ID
        else:
            # error
            print("unknown bt message; received id: {}".format(ident))
            raise ConnectionError
        return ident

    def process_write_msg(self, peer, ident):
        """
        peer: peer object
        ident: integer identifying msg

        downloader:
        client writes:
        handshake (initiates this)
        interested
        not interested
        request      

        uploader:
        client writes:
        handshake (in response to peer's handshake)
        choke
        unchoke
        bitfield
        have
        piece
        """
        ip, port = peer.address
        self.reset_keepalive(peer)
        if ident == 2:
            # client writes Interested to peer
            self.bt_state[ip].interested = 1  # client interested in peer
            if self.channel_state[ip].state == 4:
                self.channel_state[ip].state = 5
            elif self.channel_state[ip].state == 3:
                self.channel_state[ip].state = 5
            elif self.channel_state[ip].state == 50:
                self.channel_state[ip].state = 6
            elif self.channel_state[ip].state == 9:
                self.channel_state[ip].state = 6 # 8
            elif self.channel_state[ip].state == 10: # added
                self.channel_state[ip].state = 5   # client writes Interested to peer
        elif ident == 3:
            # client writes Not Interested to peer
            self.bt_state[ip].interested = 0  # client not interested in peer
            if self.channel_state[ip].state == 8:
                self.channel_state[ip].state = 9 # client writes Not Interested
        elif ident == 4:
            # client writes Have to peer
            pass # client writes Have to peer
        elif ident == 6:
            # client writes Request to peer
            if self.channel_state[ip].state == 6:
                self.channel_state[ip].state = 7
            elif self.channel_state[ip].state == 8:
                self.channel_state[ip].state = 7 # client writes Request for block
        elif ident == 7:
            # client writes Piece msg to peer
            pass # client writes Piece to peer
        elif ident == 8:
            # client writes Cancel to peer
            if self.channel_state[ip].state == 7:
                self.channel_state[ip] = 6
            elif self.channel_state[ip] in [8, 9, 10]:
                pass
            elif self.channel_state[ip] < 7 or self.channel_state[ip] == 30:
                raise ProtocolError("Cancel received in an invalid state {}".format(self.channel_state[ip].state)) # client writes Cancel to peer
        else:
            print("Client wrote Unknown message ident: {}".format(ident))
            raise ProtocolError("Client wrote Unknown message ident: {}".format(ident))

    def process_server_read(self, peer, msg):
        """
        runs state machine on incoming connections

        uploader read msgs:

        Handshake, Bitfield
        Have
        Choke
        Interested, Not Interested
        Request
        Cancel
        """
        self.logger.info('in process_server_read...')

        address = peer.address
        ip, port = address
        buf = bytearray(msg)
        ident = buf[4]

        if ident == 0:
            # server reads choke msg
            self.server_bt_state[ip].choked = 1
        elif ident == 2:
            # server reads Interested msg
            
            length = self._4bytes_to_int(buf[0:4])
            try: 
                assert length == 1
            except AssertionError:
                raise ProtocolError("Interested: bad length received from {}: expected: 1"\
                                        .format(address))
            peer.bt_state.interested = 1  # peer is interested in server
            if self.leecher_conn[ip].leecher_state == 2:
                self.leecher_conn[ip].leecher_state = 4
            # unchoke peer
            if peer.bt_state.choked and self.number_unchoked_peers < MAX_UNCHOKED_PEERS:
                self.send_unchoke_msg(peer)
                self.process_server_write(peer, UNCHOKE)
                self.number_unchoked_peers += 1
                peer.bt_state.choked = 0        
                self.logger.info('process_server_read: sent unchoke msg to {} number unchoked peers {}'\
                    .format(address, self.number_unchoked_peers))
            elif peer.bt_state.choked:
                # choke an unchoked peer to unchoke this one
                # or do nothing
                self.logger.info('process_read_msg: number of unchoked peers is {}'\
                    .format(MAX_UNCHOKED_PEERS))
        elif ident == 3:
            # server reads Not Interested mg
            length = self._4bytes_to_int(buf[0:4])
            try: 
                assert length == 1
            except AssertionError:
                raise ProtocolError("Not Interested: bad length received from {} expected: 1"\
                                        .format(address))
            peer.bt_state.interested = 0  # peer is not interested in client
            # send peer Choke msg
            if peer.bt_state.choked == 0: # unchoked
                self.send_choke_msg(peer)
                self.process_server_write(peer, CHOKE)
                peer.bt_state.choked = 1
                self.number_unchoked_peers -= 1
        elif ident == 6:
            # server reads Request msg
            msgd = self._parse_request_msg(buf)
            # check request message
            try:
                self.check_request_msg(msgd)
            except Exception as e:
                raise ProtocolError('check_request_msg: \
                received bad request from {}: {}'.format(address, e.args))

            if self.leecher_conn[ip].leecher_state == 5:
                self.leecher_conn[ip].leecher_state = 6

            # peer requests block from server
            if peer.bt_state.interested and not peer.bt_state.choked and \
                not self.server_bt_state[ip].choked:
                # send piece msg
                try:
                    piece_msg = yield from self.send_piece_msg(peer, msgd) # server sends piece message
                except Exception as e:
                    raise ProtocolError('process_server_read: \
                    error in sending {} to {}'\
                        .format(bt_messages[ident], address))
                peer.number_bytes_downloaded += msgd['length']
                self.process_server_write(peer, piece_msg)
        elif ident == 8:
            # Cancel msg
            # client must cancel associated request
            pass
        else:
            # check if handshake msg; if yes, close_connection to leecher
            if HANDSHAKE_ID == self.check_handshake_msg(peer, buf):
                raise ProtocolError('process_server_read: \
                handshake msg unexpectedly received from {}'.format(address))
            # ident Unsupported
            ident = NOTSUPPORTED_ID
        # update peer time (last time self received a msg from peer)
        peer.timer = datetime.datetime.utcnow()
        self.logger.info('process_server_read: server successfully read {} from {}'.format(bt_messages[ident], address))  

    def process_server_write(self, peer, msg):
        """
        runs state machine on incoming connections

        """
        self.logger.info('in process_server_write...')

        ip, _ = peer.address
        buf = bytearray(msg)
        ident = buf[4]

        if ident == 0:
            # server wrote Choke msg
            if self.leecher_conn[ip].leecher_state == 3:
                self.leecher_conn[ip].leecher_state = 2
            elif self.leecher_conn[ip].leecher_state == 5:
                self.leecher_conn[ip].leecher_state = 4
        elif ident == 1:
            # server wrote Unchoke msg           
            if self.leecher_conn[ip].leecher_state == 2:
                self.leecher_conn[ip].leecher_state = 3
            elif self.leecher_conn[ip].leecher_state == 4:
                self.leecher_conn[ip].leecher_state = 5
        elif ident == 4:
            # server wrote Have msg
            pass
        elif ident == 5:
            # server wrote Bitfield msg (always follows Handshake)
            if self.leecher_conn[ip].leecher_state == 10:
                self.leecher_conn[ip].leecher_state = 2
        elif ident == 7:
            # server wrote Piece msg
            if self.leecher_conn[ip].leecher_state == 6:
                self.leecher_conn[ip].leecher_state = 5
        # update client timer (last time self sends msg to peer)
        peer._client_keepalive_timer = datetime.datetime.utcnow()
        self.logger.info('process_server_write: server successfully wrote {} to {}'\
            .format(bt_messages[ident], peer.address))

    @asyncio.coroutine
    def handle_leecher(self, reader, writer):
        """
        listen for leechers
        leecher initiates connection

        read Handshake, write Handshake+bitfield
        """
        #print('in handle_leecher...')
        self.logger.info('in handle_leecher...')

        # get peer address
        ip, port = writer.get_extra_info('peername')
        peer = Peer(self.torrent, (ip, port))
        peer.reader = reader
        peer.writer = writer

        #print('in handle_leecher peername: {}:{}'.format(ip, port))
        self.logger.info('in handle_leecher peername: {}:{}'.format(ip, port))

        # expect Handshake
        try:
            msg = yield from reader.readexactly(68)
        except Exception("error in waiting for handshake from {}".format(ip)) as e:
            #print('handle_leecher: {}'.format(e.args))
            self.logger.info('handle_leecher: {}'.format(e.args))
            self._close_leecher_connection(peer)
        try:
            buf = bytearray(msg)
            msg_ident = self.check_handshake_msg(peer, buf)
        except ProtocolError("error in handshake recvd from {}".format(ip)) as e:
            self.logger.error('handler_leecher: ProtocolError: Received Handshake msg \
            with errors from ip {} {}'.format(ip, e.args))
            self._close_leecher_connection(peer)
        except Exception("other exception reading handshake from {}".format(ip)) as e:
            self.logger.error('handler_leecher: Other Exception reading Handshake msg \
            from ip {} {}'.format(ip, e.args))
            self._close_leecher_connection(peer)

        # process handshake msg
        peer.timer = datetime.datetime.utcnow()
        peer.leecher_state = 1  # conn is open
        self.leecher_conn[ip] = peer  # add leecher to leechers
        # leecher does not choke server and server is not interested in leecher
        self.server_bt_state[ip] = BTState(choked=0, interested=0) 

        # write handshake msg and bitfield msg
        # first write handshake
        peer.writer.write(self.HANDSHAKE)
        if self.leecher_conn[ip].leecher_state == 1:
            self.leecher_conn[ip].leecher_state = 10
        # then write bitfield msg
        msg = self.make_bitfield_msg()
        peer.writer.write(msg)
        self.process_server_write(peer, msg)
        # update client timer
        peer._client_keepalive_timer = datetime.datetime.utcnow()
        self.logger.info('handle_leecher: wrote Handshake to {}'.format(ip))
        self.logger.info('handle_leecher: wrote Bitfield to {}'.format(ip))
        self.logger.info('handle_leecher: wrote Handshake and Bitfield to {}'.format(ip))

        while True:
            # read next msg
            try:
                yield from self.read_leecher(peer)
            except Exception as e:
                self.logger.error(e.args)
                self._close_leecher_connection(peer)

    @asyncio.coroutine
    def read_leecher(self, peer):
        """
        seeder listens to remote peer for a message
        it then calls process_server_read
        """
        ip, _ = peer.address
        reader = peer.reader

        self.logger.info('in read_leecher')


        try:
            msg_length = yield from reader.readexactly(4)
        except (TimeoutError, OSError) as e:
            self.logger.error("read_leecher {}: caught error from reading msg[0:4]: {}".format(ip, e.args))
            msg_length = None
            pass
        except Exception as e:
            self.logger.error("read_leecher {}: Other Exception".format(ip, e.args))
            #print("read_leecher {}: Other Exception".format(ip, e.args))
            raise Exception('read_leecher: error in reading msg[0:4]') from e
        if not msg_length:
            raise Exception('read_leecher: remote forcibly closed connection')
        if msg_length == KEEPALIVE:
            peer.timer = datetime.datetime.utcnow()
            self.logger.info('read_leecher: received Keep-Alive from peer {}'.format(ip))
        else:
            try:
                msg_body = yield from reader.readexactly(self._4bytes_to_int(msg_length))
            except (TimeoutError, OSError) as e:
                self.logger.error("read_leecher {}: caught error from body {}".format(ip, e.args))
                raise ProtocolError('read_leecher: error in reading msg body \
                from {}: {}'.format(address, e.args))
            except Exception as e:
                self.logger.error("read_leecher {}: caught Other Exception {}".format(ip, e.args))
                raise ProtocolError('read_leecher: caught Other Exception in msg body \
                from {}: {}'.format(address, e.args))
            msg_ident = msg_body[0]
            if msg_ident in list(range(10)):
                # processing the read leecher msg happens here
                try:
                    yield from self.process_server_read(peer, msg_length+msg_body)
                except (ProtocolError, Exception) as e:
                    raise ProtocolError from e
            else:
                # msg_ident not supported
                self.logger.info("read_leecher: msg_id {} not supported leecher state {}"
                    .format(msg_ident, self.leecher_conn[ip].leecher_state))
                msg_ident = NOTSUPPORTED_ID
        return msg_ident






    @asyncio.coroutine
    def close_quiet_connections(self, peer):
        """
        close a connection that has timed out
        timeout is CONNECTION_TIMEOUT secs
        """
        if datetime.datetime.utcnow() - peer.timer > CONNECTION_TIMEOUT:
            # close connection
            self._close_peer_connection(peer)
    
    @asyncio.coroutine            
    def send_keepalive(self, peer):
        """sends keep_alive to peer if timer was updated > CONNECTION_TIMEOUT secs ago"""
        if datetime.datetime.utcnow() - peer._client_keepalive_timer > CONNECTION_TIMEOUT:
            peer.writer.write(KEEPALIVE)
            yield from peer.writer.drain()

            peer._client_keepalive_timer = datetime.datetime.utcnow()
            self.logger.info('wrote KEEPALIVE to {}'.format(peer.address[0]))
          
    @asyncio.coroutine
    def connect_to_peer(self, peer):
        """connect to peer address (ipv4, port) 
        
        write handshake to peer
        read handshake from peer"""

        ip, port = peer.address
        self.logger.info('connect_to_peer {}'.format(ip))
        try:
            reader, writer = yield from asyncio.open_connection(host=ip, port=port)
        except (TimeoutError, OSError, ConnectionError) as e:
            self.logger.info("connect_to_peer {}: {}".format(ip, e.args))
            self.closed_ips_cnt += 1
            del self.active_peers[ip]
            del self.channel_state[ip]
            del self.bt_state[ip]
            self.num_peers = len(self.active_peers)
            return
        except Exception as e:
            self.logger.error("connect_to_peer: {} Other Exception..{}: {}".format(ip, e.args))
            self.closed_ips_cnt += 1
            del self.active_peers[ip]
            del self.channel_state[ip]
            del self.bt_state[ip]
            self.num_peers = len(self.active_peers)
            return

        # successful connection to peer
        # write Handshake
        self._open_peer_connection(peer, reader, writer)

        self.logger.info('connect_to_peer {}: connection open'.format(ip))       
            
        while not peer.has_pieces:
            # no bitfield or have msgs yet
            self.logger.info('connnect_to_peer {}: while loop: (no bitfield/have)'.format(ip))

            try:    
                msg_ident = yield from self.read_peer(peer)

            except (ProtocolError, TimeoutError, OSError, ConnectionError) as e:
                self.logger.error("connect_to_peer: {}: reading bitfield/have {}".format(ip, e.args))
                self._close_peer_connection(peer)
            except Exception as e:
                self.logger.error('connect_to_peer:  {}: reading bitfield/have {}'.format(ip, e.args))
                self._close_peer_connection(peer)
            
            self.logger.info("connect_to_peer: {}: successfully read {}".format(ip, bt_messages[msg_ident]))

       
    def select_piece(self):
        # start the piece process
        # interested --> request --> process blocks received --> request -->...
        #
        # select a piece_index and a peer

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
            return result
        elif result == 'no open connections':
            # this should not happen
            self.logger.info('select_piece: no open connections')
            return result # connect to tracker
        elif result == 'need more pieces': # self.piece_cnts has been consumed
            # this should not happen
            self.logger.info('select_piece: self.piece_cnts have been consumed')
            return result # connect to tracker

    @asyncio.coroutine 
    def get_piece(self, rindex, rpeer):
        """
        run through protocol with rpeer
        repeat: write request/read piece - until entire piece is downloaded
        """
        rip, _ = rpeer.address
        while self.channel_state[rip].open and rindex not in self.pbuffer.completed_pieces:
            # if not interested: write Interested
            if self.channel_state[rip].open and not self.bt_state[rip].interested:
                
                # write Interested to peer
                try:
                    rpeer.writer.write(INTERESTED)
                    yield from rpeer.writer.drain()
                except Exception as e:
                    self.logger.error('get_piece: {}: error in writing Interested'.format(rip, e.args))
                    #print('get_piece: {}: error in writing Interested'.format(rip, e.args))
                    self._close_peer_connection(rpeer)
                finally:
                    if not self.active_peers:
                        # no open connections
                        return

                self.process_write_msg(rpeer, bt_messages_by_name['Interested'])
                
                self.logger.info("get_piece: {}: wrote INTERESTED state {}".\
                    format(rip, self.channel_state[rip].state))
            else:
                # channel is closed or client is already interested
                pass
        
            # if Choked, Read until Unchoked
            while self.channel_state[rip].open and self.bt_state[rip].choked:

                self.logger.info("get_piece: {}: client ready to receive Unchoke state {}"\
                    .format(rip, self.channel_state[rip].state))
                try:
                    msg_ident = yield from self.read_peer(rpeer)  
                except (ProtocolError, TimeoutError, OSError, ConnectionResetError) as e:
                    self.logger.error('get_piece: {} expected Unchoke {}'.\
                        format(rip, e.args))
                    self._close_peer_connection(rpeer)
                except Exception as e:
                    self.logger.error('get_piece: {} expected Unchoke Other Exception 2 {}'\
                        .format(rip, e.args))
                    self._close_peer_connection(rpeer)
                finally:
                    if not self.active_peers:
                        # close Task so loop stops and program can reconnect to Tracker 
                        return
                self.logger.info("get_piece: {}: received {} state {}"\
                    .format(rip, bt_messages[msg_ident], self.channel_state[rip].state))
                if bt_messages[msg_ident] == 'Unchoke':
                    break

            # channel is closed or already unchoked
                 
            # write Request and read Piece
            if self.channel_state[rip].open and \
                not self.bt_state[rip].choked and \
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
                            .format(rindex, rindex, self.channel_state[rip].state))

                    # write Request
                    offset = self.pbuffer.piece_info[rindex]['offset']
                    # update offset; invariant: offset stores value for next request
                    # rindex must be registered in piece_info first
                    self.pbuffer.piece_info[rindex]['offset'] = self._new_offset(rindex, offset)

                    self.logger.info('get_piece: index {} ready to write Request to {} begin: {} state {}'\
                        .format(rindex, rip, offset, self.channel_state[rip].state))
                    
                    # construct request msg
                    msg = self.make_request_msg(rindex, offset)
                    
                    try:
                        rpeer.writer.write(msg)
                        yield from rpeer.writer.drain()
                    except Exception as e:
                        self.logger.error('get_piece: {} write Request'.format(rip, e.args))
                        self._close_peer_connection(rpeer)
                    finally:
                        if not self.active_peers:
                            # no open connections
                            return
                    
                    self.process_write_msg(rpeer, bt_messages_by_name['Request'])
                    self.logger.info("get_piece: {}: wrote Request for index {} begin {} state {}"\
                        .format(rip, rindex, offset, self.channel_state[rip].state))
                    self.logger.info('get_piece: {} expect to receive Piece {} state {}'\
                        .format(rip, rindex, self.channel_state[rip].state))
                
                    # read until Piece
                    try:
                        msg_ident = yield from self.read_peer(rpeer)

                    except (ProtocolError, TimeoutError, OSError) as e:
                        self.logger.error('get_piece: {} expected Piece {}'.format(rip, e.args))
                        print('get_piece: {} expected Piece {}'.format(rip, e.args))
                        self._close_peer_connection(rpeer)
                    except Exception as e:
                        self.logger.error("get_piece: expect to read Piece from ip: {} {} \
                        channel_state: {} \
                        open: {} \
                        choked: {} \
                        interested: {}"\
                            .format(e.args, rip, \
                            self.channel_state[rip].state, \
                            self.channel_state[rip].open,\
                            self.bt_state[rip].choked, \
                            self.bt_state[rip].interested))
                        self._close_peer_connection(rpeer)
                    finally:
                        if not self.active_peers:
                            # no open connections
                            return

                    self.logger.info("get_piece: {}: successfully read {} index: {} state {}"\
                        .format(rip, bt_messages[msg_ident], rindex, self.channel_state[rip].state))

                    while bt_messages[msg_ident] != 'Piece' and \
                        self.channel_state[rip].open and \
                        not self.bt_state[rip].choked:
                        try:
                            msg_ident = yield from self.read_peer(rpeer)

                        except (ProtocolError, TimeoutError, OSError) as e:
                            self.logger.error('downloader: {} reading for Piece {}'.format(rip, e.args))
                            print('downloader: {} reading for Piece {}'.format(rip, e.args))
                            self._close_peer_connection(rpeer)
                        except Exception as e:
                            self.logger.error("downloader: reading for Piece  ip: {} \
                            channel_state: {} \
                            open: {} \
                            choked: {} \
                            interested: {}"\
                                .format(rip, \
                                self.channel_state[rip].state, \
                                self.channel_state[rip].open,\
                                self.bt_state[rip].choked, \
                                self.bt_state[rip].interested))
                            self._close_peer_connection(rpeer)
                        finally:
                            if not self.active_peers:
                                # no open connections
                                return
                        self.logger.info("get_piece: {}: successfully read {} state {}"\
                            .format(rip, bt_messages[msg_ident], self.channel_state[rip].state))
                    # received Piece msg or Choke msg or Exception (if exception: client closes connection to rip)
                    # top of while loop: if not all_pieces(), get a piece index 
                    # (could be the same piece index but with a new offset)
            else:
                # channel is closed, choked, or all pieces from peers are complete
                pass
            
        ## channel is closed or piece is completely downloaded
        ## for each open peer (if any): reset Interested to Not Interested
        #try:
        #    rpeer.writer.write(NOT_INTERESTED)
        #    yield from rpeer.writer.drain()
        #except Exception as e:
        #    logging.debug('get_piece: {} error in writing Not Interested'.format(rip))
        #    print('get_piece: {} error in writing Not Interested'.format(rip))
        #    self._close_peer_connection(rpeer)
        #finally:
        #    if not self.active_peers:
        #        # no open connections
        #        return

        #self.process_write_msg(rpeer, bt_messages_by_name['Not Interested'])
        #print('get_piece: successfully wrote Not Interested to {} state {}'\
        #    .format(rip, self.channel_state[rip].state))
        #logging.debug('get_piece: successfully wrote Not Interested to {} state {}'\
        #    .format(rip, self.channel_state[rip].state))
        return
                            
                
    @asyncio.coroutine
    def _read_handshake(self, peer):
        ip, _ = peer.address
        # read Handshake from peer
        self.logger.info('_read_handshake: about to readexactly 68 handshake bytes')
        
        try:
            msg_hs = yield from peer.reader.readexactly(68)      # read handshake msg
        except (ConnectionError, ProtocolError, ConnectionResetError) as e:
            self.logger.error('_read_handshake {} Not ConnectionResetError'.format(ip))
            raise e
        except Exception as e:
            self.logger.error('_read_handshake {} Other Exception'.format(ip))
            raise e
        # received Handshake from peer
        msg_ident = yield from self.process_read_msg(peer, msg_hs)
        self.logger.info('received {} from peer {} channel state: {}'\
            .format(bt_messages[msg_ident], ip, self.channel_state[ip].state))
        return msg_ident
                             
    @asyncio.coroutine
    def read_peer(self, peer):
        """
        reads msg from peer and processes it

        exceptions are re-raised to the calling function
        """
        ip, _ = peer.address
        reader = peer.reader

        self.logger.info('in read_peer')
        
        if self.channel_state[ip].open == 1 and self.channel_state[ip].state == 1:
            self.logger.info('read_peer: to read Handshake from {}'.format(ip))
            try: 
                msg_ident = yield from self._read_handshake(peer)

            except ConnectionError as e:
                self.logger.error('read_peer {}: {}'.format(ip, e.args))
                raise e
            except Exception as e:
                self.logger.info('read_peer {}: read Handshake error {}'.format(ip, e.args))
                raise e

            self.logger.info('read_peer: Received Handshake from {}'.format(ip))
            return msg_ident
        else:
            try:
                msg_length = yield from reader.readexactly(4)

            except (ProtocolError, TimeoutError, OSError) as e:
                self.logger.error("read_peer {}: caught error from reading msg_length: {}".format(ip, e.args))
                raise e
            except Exception as e:
                self.logger.error("read_peer {}: caught Other Exception 2:  {}".format(ip, e.args))
                raise e
            if msg_length == KEEPALIVE:
                peer.timer = datetime.datetime.utcnow()
                self.logger.info('read_peer: received Keep-Alive from peer {}'.format(ip))
                msg_ident = KEEPALIVE_ID
            else:
                try:
                    msg_body = yield from reader.readexactly(self._4bytes_to_int(msg_length))

                except (ProtocolError, TimeoutError, OSError) as e:
                    self.logger.error("read_peer {}: caught error from body {}".format(ip, e.args))
                    raise e
                except Exception as e:
                    self.logger.error("read_peer {}: caught Other Exception {}".format(ip, e.args))
                    raise e
                msg_ident = msg_body[0]
                if msg_ident in list(range(10)) or msg_ident == KEEPALIVE_ID:

                    # processing the read msg happens here
                    yield from self.process_read_msg(peer, msg_length + msg_body)

                    self.logger.info('read_peer: received {} from peer {} channel state: {}'\
                        .format(bt_messages[msg_ident], \
                        ip, self.channel_state[ip].state))
                else:
                    # msg_ident not supported
                    self.logger.info("read_peer: msg_id {} not supported \
                    channel state: {}"\
                        .format(msg_ident, self.channel_state[ip].state))
                    msg_ident = NOTSUPPORTED_ID
            self.logger.info('read_peer: successfully read {} from peer {}'\
                .format(bt_messages[msg_ident], peer.address))    
            return msg_ident 