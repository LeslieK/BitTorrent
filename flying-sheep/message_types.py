"""
This module implements a BitTorrent client without extensions.
"""
from bcoding import bdecode, bencode
from collections import defaultdict, Counter
import math
import hashlib
import array
import bisect
import datetime
import sys, os, socket
#import aiohttp
import requests
import asyncio

from bt_utils import my_peer_id, sha1_info, make_handshake
from bt_utils import rcv_handshake

from bt_utils import HASH_DIGEST_SIZE

import logging
logger = logging.getLogger('asyncio')
logging.basicConfig(filename="bittorrent.log", filemode='w', level=logging.DEBUG, format='%(asctime)s %(message)s')
logging.captureWarnings(capture=True)

DEFAULT_BLOCK_LENGTH = 2**14
MAX_PEERS = 50
BLOCK_SIZE = 16384
MAX_BLOCK_SIZE = b'\x04\x00\x00\x00' # 2**14 == 16384
NUMWANT = 5  # GET parameter to tracker
BUFFER_SIZE = 5
CHANNEL_TIMEOUT = datetime.timedelta(seconds=90)
HANDSHAKE_ID = 100
KEEPALIVE_ID = 200
NOTSUPPORTED_ID = 300

PORTS = [i for i in range(6881, 6890)]
events = ['started', 'stopped', 'completed']

KEEPALIVE = b'\x00\x00\x00\x00'
CHOKE = b'\x00\x00\x00\x01' + b'\x00'
UNCHOKE = b'\x00\x00\x00\x01' + b'\x01'
INTERESTED = b'\x00\x00\x00\x01' + b'\x02'
NOT_INTERESTED = b'\x00\x00\x00\x01' + b'\x03'

class ConnectionError(Exception):
    pass

class ConnectionResetError(Exception):
    pass

class BufferFullError(Exception):
    pass


class ProtocolError(Exception):
    pass

class BTState:
    def __init__(self):
        self.choked = 1
        self.interested = 0

class ChannelState:
    def __init__(self):
        self.open = 0
        self.state = 0

bt_messages = {0: 'Choke',
               1: 'Unchoke',
               2: 'Interested',
               3: 'Not Interested',
               4: 'Have',
               5: 'Bitfield',
               6: 'Request',
               7: 'Piece',
               8: 'Cancel',
               9: 'Port',
               20: 'Extended',
               HANDSHAKE_ID: 'Handshake',
               KEEPALIVE_ID: 'KeepAlive',
               NOTSUPPORTED_ID: 'Not Supported'}

bt_messages_by_name = {name: ident for ident, name in bt_messages.items()}

class PieceBuffer(object):
    """
    stores the bytes in a piece
    stores up to BUFFER_SIZE pieces
    """
    def __init__(self, torrent):
        self.torrent = torrent
        self.buffer ={row: array.array('B', bytearray(self.torrent.torrent['info']['piece length'])) for row in range(BUFFER_SIZE)}
        self.free_rows = {i for i in range(BUFFER_SIZE)}
        self.completed_pieces = set()  # set of completed pieces (indices); store as a client attribute?
        # {'piece index': 20, 'all_bytes_received': False, 'hash_verifies': False, 'bitfield': 1111100000, 'offset': 0x8000}
        self.piece_info = {}
        

    def is_full(self):
        return not self.free_rows  

    def insert_bytes(self, piece_index, begin, block):
        """
        inserts bytes into the array for piece_index

        piece_index: int
        begin: int
        block: sequence of bytes (bytearray)
        """
        if not self.piece_info or piece_index not in self.piece_info:
            logging.debug("{} not in buffer. Registering piece...".format(piece_index))
            print("{} not in buffer. Registering piece...".format(piece_index))
            self._register_piece(piece_index)

        row = self.piece_info[piece_index]['row']
        # insert block of bytes
        ablock = array.array('B', block)

        logging.debug('insert_bytes: length(ablock) = {}, {}'.format(len(ablock), ablock[:5]))
        print('insert_bytes: length(block) = {}, {}'.format(len(ablock), ablock[:5]))

        self.buffer[row][begin:begin+len(ablock)] = ablock

        logging.debug('insert_bytes: PieceBuffer buffer[{}] begin: {}'
            .format(row, begin))
        print('insert_bytes: PieceBuffer buffer[{}] begin: {}'
            .format(row, begin))

        # update bitfield (each bit represents a block in the piece)
        self._update_bitfield(piece_index)

        # check if all blocks received
        if self._is_all_blocks_received(piece_index):
            self.piece_info[piece_index]['all_blocks_received'] = True
            if self._is_piece_hash_good(piece_index):
                # all bytes received
                # piece hash matches torrent hash
                self.piece_info[piece_index]['hash_verifies'] = True
                self.completed_pieces.add(piece_index)  # set of piece indices
                return 'done'
            else:
                # piece hash does not match torrent match
                return 'bad hash'
        else:
            # not all pieces received
            return 'not done'

    def reset(self, piece_index, free_row=False):
        """
        reset buffer row to all 0s
        reset bitfield to all 0s
        if free_row: del piece_index from buffer row; add row to set of available rows
        if not free_row: reset row but keep row in buffer
        """
        row = self.piece_info[index]['row']
        self.buffer[row][:] = bytearray(self.torrent.torrent['info']['piece length'])
        self.piece_info[piece_index]['bitfield'] = self._init_bitfield(piece_index)
        if free_row:
            del self.piece_info[piece_index]
            self.free_rows.add(row)  # add row to set of available rows

    def pieces_in_buffer(self):
        """returns a generator object that generates indices of pieces in buffer"""
        return (piece_index for piece_index in self.piece_info.keys())

    def is_piece_complete(self, piece_index):
        """
        self.completed_pieces: set of all completed pieces
        """
        return piece_index in self.completed_pieces


    def _register_piece(self, piece_index):
        try:
            row = self.free_rows.pop()
        except KeyError as e:
            logging.debug("Buffer is full")
            raise BufferFullError("Buffer is full")
        else:
            self.piece_info[piece_index] = {'row': row,
                                    'all_blocks_received': False,
                                    'hash_verifies': False,
                                    'bitfield': self._init_bitfield(piece_index),
                                    'offset': 0
                                    }

    def _sha1_hash(self, piece_index):
        """hash the piece bytes"""
        try:
            row = self.piece_info[piece_index]['row']
        except KeyError:
            print("piece index {} is not in buffer".format(piece_index))
            raise KeyError
        else:
            sha1 = hashlib.sha1()
            if piece_index != self.torrent.LAST_PIECE_INDEX:
                sha1.update(self.buffer[row])
            else:
                sha1.update(self.buffer[row][:self.torrent.last_piece_length])
            return sha1.digest()

    def _is_piece_hash_good(self, piece_index):
        torrent_hash_value = self.torrent.get_hash(piece_index)
        piece_hash_value = self._sha1_hash(piece_index)
        return torrent_hash_value == piece_hash_value
       
    def _is_all_blocks_received(self, piece_index):
        """
        returns True if bitfield has no "0"
        """
        bf = bin(self.piece_info[piece_index]['bitfield'])[3:]
        return not('0' in bf)

    def _init_bitfield(self, piece_index):
        """
        init bitfield for a buffer row 
        all bits initialized to 0
        rightmost bit (LSB): block 
        """
        try:
            if piece_index != self.torrent.number_pieces - 1:
                number_of_blocks = self.torrent.piece_length // BLOCK_SIZE 
            else:
                number_of_blocks = self.torrent.last_piece_length // BLOCK_SIZE
        except Exception as e:
            print(e.args) 
             
        field = 1 << number_of_blocks
        return field
    
    def _update_bitfield(self, piece_index):
        """
        update bitfield for buffer row
        each bit represents a block
        rightmost bit (LSB): block 0
        """
        block_number = self.piece_info[piece_index]['offset'] // BLOCK_SIZE

        bfs = bin(self.piece_info[piece_index]['bitfield'])[3:]
        length = len(bfs)
        logging.debug('_update_bitfield (pbuffer): {}'.format(bfs[length-1-block_number]))
        print('_update_bitfield (pbuffer): {}'.format(bfs[length-1-block_number]))

        self.piece_info[piece_index]['bitfield'] |= 1 << block_number

        bfs = bin(self.piece_info[piece_index]['bitfield'])[3:]
        length = len(bfs)
        logging.debug('_update_bitfield (pbuffer): {}'.format(bfs[length-1-block_number]))
        print('_update_bitfield (pbuffer): {}'.format(bfs[length-1-block_number]))


class Message(object):
    def __init__(self, name):
        self.name = name
        self.ident = bt_messages[name]['id']
        self.length = bt_messages[name]['length']
        
    def is_valid():
        raise NotImplementedError

    def send(to_peer, payload=None):
        """ to_peer: tuple([ip, port])"""
        raise NotImplementedError

class TorrentWrapper(object):
    """
    for a multi-file torrent, the piece indices are in the order of the files in the 'files' element.
    For ex, if 1st file in the 'files' list has 6 pieces and the 2nd file in the 'files' list has 5 pieces, 
    the first 6 of the 11 pieces are for file 1. The next 5 pieces are for file 2.
    """
    def __init__(self, metafile):
        with open(metafile, 'rb') as f:
            self.torrent = bdecode(f)
        self.INFO_HASH = sha1_info(self.torrent)
        self.TRACKER_URL = self.announce()
        self.piece_length = self.torrent['info']['piece length']
        self.last_piece_length = self._length_of_last_piece()
        self.number_pieces = self._num_pieces()
        self.LAST_PIECE_INDEX = self.number_pieces - 1

        self.file_meta = self.file_info()
        self.file_boundaries_by_byte_indices = self.list_of_file_boundaries()

    def announce(self):
        """
        torrent: the dict in the .torrent file
        returns announce url: url of tracker
        """
        return self.torrent['announce']


    def get_hash(self, index, digest_size=HASH_DIGEST_SIZE):
        # hashes = io.BytesIO(torrent['info']['pieces']).getbuffer().tobytes()
        hashes = self.torrent['info']['pieces']
        hash_index = index * digest_size
        return hashes[hash_index:hash_index + digest_size]

    def _num_pieces(self, digest_size=HASH_DIGEST_SIZE):
        return math.ceil(self.total_file_length() / self.piece_length)

    def _length_of_last_piece(self):
        return self.total_file_length() - self.piece_length * (self._num_pieces() - 1)

    def total_file_length(self):
        """returns the number of bytes across all files"""
        return sum([file['length'] for file in self.torrent['info']['files']])

    def is_multi_file(self):
        """return True if more than 1 file"""
        return 'files' in self.torrent['info']

    def file_info(self):
        """
        if multi-file:
        returns [{'length': len1, 'name': dir1/dir2/fileA]}, {'length': len2, 'name': 'fileB'}, ... ]
        if single-file:
        return [{'length': nn, 'name': filename, 'num_pieces': <number pieces in this file>}]
        """
        piece_length = self.torrent['info']['piece length']
        if self.is_multi_file():
            dict = {}
            files = self.torrent['info']['files']
            file_meta = [{'length': file['length'],
                          'name': os.path.join(*file['path']),
                          'num_pieces': # beginning and/or ending piece might span adjacent files
                          math.ceil(file['length'] / piece_length)}
                          for file in files]      
        else:
            name = self.torrent['info']['name']
            file_size = self.torrent['info']['length']
            file_meta = [{'length': self.torrent['info']['length'],
                          'name': self.torrent['info']['name'],
                          'num_pieces': 
                          math.ceil(self.torrent['info']['length']/piece_length)}]
        return file_meta

    def list_of_file_boundaries(self):
        """creates a list of file boundaries;
        boudaries are byte indices
        
        ex: [len(file0), len(file0) + len(file1)]
        file0: 0 <= byte indices < len(file0)
        file1: len(file0) <= byte indices < len(file0) + len(file1)
        res: [100, 100+51, 100+51+205]
        """
        res = []
        list_of_file_lengths = [file['length'] for file in self.file_meta]
        partial_sum = 0
        for i in range(len(list_of_file_lengths)):
            partial_sum += list_of_file_lengths[i]
            res.append(partial_sum)
        return res[:]

class Client(object):
    """client uploads/downloads 1 torrent (1 or more files)"""

    def __init__(self, torrent):
        self.peer_id = my_peer_id()
        self.torrent = torrent
        self.tracker_response = {}
        self.total_files_length = torrent.total_file_length()
        self.files = self.torrent.file_info()  # list of dicts [{'length': <>, 'name': <>, 'num_pieces': <>}, ...]
        self.num_peers = 0
        self.num_bytes_uploaded = 0
        self.num_bytes_downloaded = 0
        self.num_bytes_left = self.total_files_length
        self.message = ""
        self.bitfield = self.init_bitfield() # sets all bits to 0; piece 0 is leftmost bit (MSB)
        self.piece_length = self.torrent.torrent['info']['piece length']
        self.last_piece_length = self._length_of_last_piece()
        self.pbuffer = PieceBuffer(torrent)        # PieceBuffer object
        self.buffer = self.pbuffer.buffer          # buffer attrib of PieceBuffer object
        self.output_fds = self.open_files()
        self.HANDSHAKE = make_handshake(self.torrent.INFO_HASH, self.peer_id)
        self.active_peers = {}  # updated with each tracker response
        self.bt_state = {}
        self.channel_state = {}
        self.piece_to_peers = defaultdict(set)
        self.peer_to_pieces = defaultdict(set)
        #self.timer = datetime.datetime.utcnow() 
        self.tracker_timer = datetime.datetime.utcnow()
        self.tracker_request_interval = datetime.timedelta(seconds=0)
    
    def connect_to_tracker(self, port, numwant=NUMWANT):
        """send GET request; parse self.tracker_response
        
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

        # blocking
        r = requests.get(self.torrent.TRACKER_URL, params=http_get_params)

        # reset client's tracker timer
        self.tracker_timer = datetime.datetime.utcnow()

        tracker_resp = r.content
        self.tracker_response = bdecode(tracker_resp)
        if 'failure reason' in self.tracker_response:
            self.message = self.tracker_response['failure reason']
            logging.debug('tracker failure: {}'.\
                format(self.tracker_response['failure reason']))
        elif 'warning message' in self.tracker_response:
            self.message  = self.tracker_response['warning message']
            logging.debug('tracker warning: {}'.\
                format(self.tracker_response['warning message']))
        else:
            success = self._parse_tracker_response()
            self.message = '{}: parsed tracker response'.format(success)
            logging.debug('{}: parsed tracker response'.format(success))
            print('{}: parsed tracker response'.format(success))
            return success  # True or False
        return False

    def _parse_tracker_response(self):
        """parses tracker response
        
        updates active_peers, bt_state, channel_state"""

        if 'failure reason' in self.tracker_response:
            self.message = self.tracker_response['failure reason']
            return False
        elif 'warning message' in self.tracker_response:
            self.message  = self.tracker_response['warning message']
            return False
        else:
            self._list_of_peers()  # updates active_peers, bt_state, channel_state
            self.num_peers = len(self.active_peers)
            self.tracker_request_interval = \
                datetime.timedelta(seconds=self.tracker_response.get('min interval', 
                                          self.tracker_response['interval']))
            return True
            
    def _list_of_peers(self):
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

    def open_peers(self):
        if not self.active_peers:
            return {}
        else:
            return {ip:peer for ip, peer in self.active_peers.items() if self.channel_state[ip].open}

    def reset_keepalive(self, peer):
        peer._client_keepalive_timer = datetime.datetime.utcnow()

    def _close_peer_connection(self, peer):
        """clean-up after failed connection to peer"""
        ip, _ = peer.address
        # close channel state
        self.channel_state[ip].open = 0
        self.channel_state[ip].state = 0
        self.bt_state[ip].choked = 1
        self.bt_state[ip].interested = 0
        # remove ip from data structures
        #if ip in self.peer_to_pieces:
        #    self._remove_ip_piece_peer_maps(ip)
        # remove from self.active_peers
        del self.active_peers[ip]
        #if not self.active_peers:
        #    # no peers left
        #    logging.debug('{}: last connection closed... no peers left'.format(ip))
        #    return False  # closed the last peer connection
        # close connection
        peer.reader.set_exception(None)
        peer.writer.close()
        logging.debug('{}: cleaned up after closing connection'.format(ip))
        print('{}: cleaned up after closing connection'.format(ip))
        return True # not sure why

    def _open_peer_connection(self, peer, reader, writer):
        """
        sets channel_state[ip]
        attaches reader, writer to peer object
        """
        logging.debug('_open_peer_connection to {}'.format(peer.address[0]))
        self.send_keepalive(peer)       

        ip, _ = peer.address
        peer.reader = reader
        peer.writer = writer
        self.channel_state[ip].open = 1
        self.channel_state[ip].state = 1
        logging.debug('write Handshake to {}'.format(ip))
        peer.writer.write(self.HANDSHAKE)
        self.reset_keepalive(peer)

    #def _remove_ip_piece_peer_maps(self, ip):
    #    """
    #    removes the ip from the maps:
    #    self.peer_to_pieces
    #    self.piece_to_peers
    #    (map stores peer as an ip address not peer object)
    #    """
    #    if ip in self.peer_to_pieces:
    #        for pindex in self.peer_to_pieces[ip]:
    #            if len(self.piece_to_peers[pindex]) == 1:
    #                del self.piece_to_peers[pindex]
    #            else:
    #                self.piece_to_peers[pindex].remove(ip)
    #        del self.peer_to_pieces[ip]

    #def _remove_index_piece_peer_maps(self, index):
    #    """
    #    removes the piece_index from the maps:
    #    self.piece_to_peers
    #    self.peer_to_pieces
    #    (map stores 'ip address' not peer object)
    #    """
    #    for ip in self.piece_to_peers[index]:
    #        self.peer_to_pieces[ip].remove(index)
    #        # remove keys with empty values
    #        if not self.peer_to_pieces[ip]:
    #            del self.peer_to_pieces[ip]
    #    del self.piece_to_peers[index]

    def _length_of_last_piece(self):
        return self.total_files_length - self.piece_length * (self.torrent.number_pieces - 1)

    def _piece_length(self, piece_index):
        bytes_in_piece = self.piece_length if piece_index != self.torrent.LAST_PIECE_INDEX \
            else self.last_piece_length
        return bytes_in_piece

    def _number_of_bytes_left(self):
        return self.total_files_length - self.num_bytes_downloaded

    def open_files(self):
        """open files for writing pieces to file"""
        fds = [open(file['name'], mode='wb', ) for file in self.files]
        return fds

    def write_buffer_to_file(self, piece_index=None):
        """
        write piece in buffer to the filesystem using the pathname
        row: indicates buffer row (i.e., piece) to write to file
        start: start byte position wrt piece
        offset: offset into file from head
        bytes_left: number of bytes in piece that still need to be written to file
        """
        if piece_index:
            # write 1 piece to buffer
            self._write_piece_to_file(self, index)
        else:
            # write entire buffer to file(s)
            for piece_index in self.pieces_in_buffer():
                self._write_piece_to_file(self, piece_index)         

    def _write_piece_to_file(self, piece_index):
        row = self.pbuffer.piece_info[piece_index]
        # write piece to 1 or more files
        bytes_left = self.piece_length                
        start = piece_index * self.torrent.piece_length
        file_index = bisect(self.file_boundaries_by_byte_indices, start)
        reference = 0 if file_index == 0 else self.file_boundaries_by_byte_indices[file_index]
        offset = start - reference 

        while bytes_left > 0:
            m = min((self.files[file_index]['length'] - offset, 0), (bytes_left, 1))
            if m[1] == 0:  # bytes_left span multiple files
                self._write(self.output_fds[file_index], offset, bytes_left - m[0], row)
                start += bytes_left - m[0]
                offset = 0
                bytes_left -= m[0]
                file_index += 1
            else:          # bytes_left are in a single file
                self._write(self.files[file_index], offset, bytes_left, row)
                bytes_left = 0
        self.pbuffer.reset(piece_index, free_row=True)

    def _write(self, fd, offset, num_bytes, row):  # bug here: start not a param; offset is the param
        fd.write(self.buffer[row][start:start+num_bytes])

    def _close_fds(self):
        [fd.close() for fd in self.output_fds]
        return

    def _helper_comp(self, an_index):
        """returns the complement of an_index"""
        num_bytes=math.ceil(self.torrent.number_pieces / 8)
        max_i = num_bytes * 8 - 1
        return max_i - an_index

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
        bitfield: 0xb1---- ---- (ignore leftmost 1)
        return a bitstring
        """    
        bitstring = bin(self.bitfield)[2:]
        try:
            assert len(bitstring) % 8 == 1
        except AssertionError as e:
            print(e.args)
        num_bytes = math.ceil(len(bitstring) / 8)
        bitfield_bytes = (self.bitfield).to_bytes(num_bytes, byteorder='big')
        return bitfield_bytes[1:] # leftmost 1 is ignored here

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

    def make_bitfield_msg(self):
        """convert self.bitfield from int to a bit string"""
        num_bytes = math.ceil(self.torrent.number_pieces / 8)
        length = self._int_to_4bytes(num_bytes)
        ident = b'5'
        bitfield = self.int_to_bitstring(self.bitfield)
        return length + ident + bitfield

    def get_indices(self, bitfield):
        b = bytearray(bitfield)
        b = ''.join([bin(x)[2:] for x in b])
        return {i for i, x in enumerate(b) if x == '1'}

    def make_have_msg(self, piece_index):
        """
        client makes msg after it downloads piece_index and it verifies hash
        """
        length = self._int_to_4bytes(5)
        ident = b'4'
        return length + ident + self._int_to_4bytes(piece_index)

    def make_cancel_msg(self, request_msg):
        buf = bytearray(request_msg)
        buf[4] = 8  # bytearray requires int values
        return bytes(buf)

    def make_request_msg(self, piece_index, begin_offset, block_len=DEFAULT_BLOCK_LENGTH):
        """only send request to a peer that has piece_index"""

        length = b'\x00\x00\x00\x0d' # 13
        ident = b'\x06'
        index = self._int_to_4bytes(piece_index)
        begin = self._int_to_4bytes(begin_offset)
        num_bytes = self._int_to_4bytes(block_len)       
        return length + ident + index + begin + num_bytes

    def make_piece_msg(self, piece_index, begin_offset, block_size=MAX_BLOCK_SIZE):
        # convert piece_index to (file_index, offset) to get bytes from appropriate file
        file_index, offset = _piece_index_to_file_index(piece_index)

        ident = b'7'
        begin = _int_to_4bytes(begin_offset)
        index = _int_to_4bytes(piece_index)

        i = self.piece_indices[file_index].index(offset)  # returns the array index of the requested piece_index
        ablock = self.piece_bytes[file_index][i][begin_offset:begin_offset + block_size].tobytes() # ablock will really come from a file
        length = 9 + len(ablock)
        return length + ident + index + begin + ablock

    def rcv_handshake_msg(self, buf):
        """buf is a bytearray of the msg bytes"""
        msgd = {}
        msgd['pstrlen'] = buf[0]
        msgd['pstr'] = b'BitTorrent protocol'
        msgd['reserved'] = buf[20:28]
        msgd['info_hash'] = buf[28:48]
        msgd['peer_id'] = buf[48:68]
        return msgd

    def _get_next_piece(self):
        """
        yield (piece_index, ip)

        1. find connected peers (self.channel_state[ip].open == 1)
        2. peer.has_pieces is a set of piece indices
        3. get most rare piece that client doesn't already have
        """
        result = Counter() 
        cc = [Counter(peer.has_pieces) for peer in self.active_peers.values()]
        for i in range(len(cc)):
            result += cc[i]
        rarest_index, cnt = result.most_common()[-1]
        set_of_ips = self.piece_to_peers[rarest_index]
        open_ips = {ip for ip in set_of_ips if self.channel_state[ip].open}
        ip = open_ips.pop()
        return rarest_index, ip

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
        # update data structures
        self.active_peers[ip].has_pieces.add(msgd['piece index'])
        self.piece_to_peers[msgd['piece index']].add(ip)
        self.peer_to_pieces[ip].add(msgd['piece index'])
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
        for piece in self.active_peers[ip].has_pieces:
            self.piece_to_peers[piece].add(ip)
            self.peer_to_pieces[ip].add(piece)
        return msgd

    def rcv_request_msg(self, buf):
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
        length = self._4bytes_to_int(buf[:4])
        ident = buf[4]
        index = self._4bytes_to_int(buf[5:9])
        begin = self._4bytes_to_int(buf[9:13])
        block = buf[13:]
        try:
            result = self.pbuffer.insert_bytes(index, begin, block)
        except Exception as e:
            print(e.args)
        if result is 'done':
            # piece is complete and hash verifies
            try:
                self.num_bytes_downloaded += self._piece_length(index)
            except Exception as e:
                print(e.args)
            self.num_bytes_left -= self.num_bytes_downloaded
            self.update_bitfield([index])  # updates self.bitfield
            # update data structures: piece:{ips} and ip:{pieces}
            # self._remove_index_piece_peer_maps(index) 
            # make/send have msg for this piece; send to ips that don't have it
            self.pbuffer.piece_info[index]['offset'] = 0
        elif result is 'bad hash':
            # all blocks received and hash doesn't verify
            self.pbuffer.reset(index)
            # make/send request msg for this piece
            self.pbuffer.piece_info[index]['offset'] = 0
        else:
            # not all blocks received
            # increment offset
            self.pbuffer.piece_info[index]['offset'] = begin + length - 9 # lenth of block = length - 9

    def process_read_msg(self, peer, msg):
        """process incoming msg from peer - protocol state machine
        
        peer: peer instance
        msg: bit torrent msg
        """
        if not msg:
            return

        buf = bytearray(msg)
        ip, port = peer.address
        
        try:
            assert buf[1:20].decode() == 'BitTorrent protocol'
        except UnicodeDecodeError:
           pass
        except AssertionError:
            pass
        else:
            try:
                assert buf[0] == 19
            except AssertionError:
                raise ConnectionError("received handshake msg with length {}; should be 19".format(buf[0].decode()))
            else:
                #  handshake messaage
                msgd = self.rcv_handshake_msg(buf)
                try:
                    # check info_hash
                    assert msgd['info_hash'] == self.torrent.INFO_HASH
                except AssertionError:
                    raise ConnectionError("peer is not part of torrent: expected hash: {}"\
                        .format(self.torrent.INFO_HASH))
                else:
                    # check protocol
                    if self.channel_state[ip].state == 1:
                        self.channel_state[ip].state = 2
                    else:
                        raise ProtocolError("expected Handshake in state 1; \
                        received in state {}".format(self.channel_state[ip].state))
                    # set peer_id of peer
                    peer.peer_id = msgd['peer_id']
                    # reset peer timer
                    peer.timer = datetime.datetime.utcnow()
                    ident = HANDSHAKE_ID  # identifies handshake
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
            else:
                peer.timer = datetime.datetime.utcnow()
                self.bt_state[ip].choked = 1  # peer choked client
                if self.channel_state[ip].state == 30:
                    self.channel_state[ip].state = 3
                elif self.channel_state[ip].state == 6:
                    self.channel_state[ip].state = 5
                elif self.channel_state[ip].state == 7:
                    self.channel_state[ip].state = 5
                elif self.channel_state[ip].state == 9:
                    self.channel_state[ip].state = 10

        elif ident == 1:
            #  unchoke message
            length = self._4bytes_to_int(buf[0:4])
            try: 
                assert length == 1
            except AssertionError:
                raise ConnectionError("Unchoke: bad length  received: {} expected: 1"\
                                      .format(length))
            else:
                peer.timer = datetime.datetime.utcnow()
                self.bt_state[ip].choked = 0  # peer unchoked client
                if self.channel_state[ip].state == 3:
                    self.channel_state[ip].state = 30
                elif self.channel_state[ip].state == 5:
                    self.channel_state[ip].state = 6
                elif self.channel_state[ip] == 10:
                    self.channel_state[ip] = 9
        elif ident == 2:
            #  interested message
            length = self._4bytes_to_int(buf[0:4])
            try: 
                assert length == 1
            except AssertionError:
                raise ConnectionError("Interested: bad length  received: {} expected: 1"\
                                      .format(length))
            else:
                peer.timer = datetime.datetime.utcnow()
                peer.bt_state.interested = 1  # peer is interested in client
        elif ident == 3:
            #  not interested message
            length = self._4bytes_to_int(buf[0:4])
            try: 
                assert length == 1
            except AssertionError:
                raise ConnectionError("Not Interested: bad length  received: {} expected: 1"\
                                      .format(length))
            else:
                peer.timer = datetime.datetime.utcnow()
                peer.bt_state.interested = 0  # peer is not interested in client
        elif ident == 4:
            #  have message
            try:
                assert self.channel_state[ip].state != 1
            except AssertionError:
                raise ProtocolError("cannot receive Have msg in state 1")
            else:
                peer.timer = datetime.datetime.utcnow()
                msgd = self.rcv_have_msg(ip, buf)
                if self.channel_state[ip].state == 4:
                    self.channel_state[ip].state = 3   
        elif ident == 5:
            #  bitfield message
            try:
                assert self.channel_state[ip].state == 2
            except AssertionError:
                raise ProtocolError(
                    'Bitfield received in state {}. Did not follow Handshake.'\
                     .format(self.channel_state[ip].state))
            else:
                peer.timer = datetime.datetime.utcnow()
                msgd = self.rcv_bitfield_msg(ip, buf)  # set peer's bitfield
                self.channel_state[ip].state = 4
        elif ident == 6:
            #  request message
            peer.timer = datetime.datetime.utcnow()
            pass
        elif ident == 7:
            #  piece message
            peer.timer = datetime.datetime.utcnow()
            self.rcv_piece_msg(msg)
            pass
        elif ident == 8:
            #  cancel message
            peer.timer = datetime.datetime.utcnow()
            pass
        elif ident == 9:
            #  port message
            peer.timer = datetime.datetime.utcnow()
            pass
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

        generate msgs to send
        downloader:
        handshake
        interested
        not interested
        request      

        uploader:
        choke
        unchoke
        bitfield
        have
        piece
        """
        ip, port = peer.address
        if ident == 2:
            # client writes Interested to peer
            self.bt_state[ip].interested = 1  # client interested in peer
            if self.channel_state[ip].state == 4:
                self.channel_state[ip].state = 5
            elif self.channel_state[ip].state == 3:
                self.channel_state[ip].state = 5
        elif ident == 3:
            # client writes Not Interested to peer
            self.bt_state[ip].interested = 0  # client not interested in peer
            if self.channel_state[ip].state == 8:
                self.channel_state[ip].state = 9
        elif ident == 6:
            # client writes Request to peer
            if self.channel_state[ip].state == 6:
                self.channel_state[ip].state = 7
        elif ident == 8:
            # client writes Cancel to peer
            if self.channel_state[ip].state == 7:
                self.channel_state[ip] = 6
            elif self.channel_state[ip] in [8, 9, 10]:
                pass
            elif self.channel_state[ip] < 7 or self.channel_state[ip] == 30:
                raise ProtocolError("Cancel received in an invalid state {}".format(self.channel_state[ip].state))
        else:
            print("Client wrote Unknown message ident: {}".format(ident))
            raise ProtocolError("Client wrote Unknown message ident: {}".format(ident))

    @asyncio.coroutine    
    def downloader(self):
        """this is a centralized task that selects the next piece
        and runs the bt protocol state machine"""

        count = 0
        while not self.open_peers():
            logging.info('downloader: while loop: no open_peers()')
            print('downloader: while loop: no open_peers()')
            count += 1
            yield

        number_open_peers = len(self.open_peers())
        print('After {} tries, {} connections are open'.format(count, number_open_peers))
        count = 0

        while not any(peer.has_pieces for peer in self.active_peers.values() if self.active_peers):
            # no bitfield or have msgs yet
            logging.info('downloader: while loop: (no bitfield/have)')
            print('downloader: while loop: (no bitfield/have)')
            
            for ip, peer in self.open_peers().items():
                # for every open peer
                logging.debug('downloader:  {}, open={}'.format(ip, self.channel_state[ip].open))
                self.send_keepalive(peer)

                try:
                    logging.info('downloader: read_peer: {}'.format(ip))
                    msg_ident = yield from self.read_peer(peer)

                except (ProtocolError, TimeoutError, OSError, ConnectionResetError) as e:
                    logging.debug("downloader: {}: reading bitfield/have {}".format(ip, e.args))
                    self._close_peer_connection(peer)
                except Exception as e:
                    logging.debug('downloader:  {}: reading bitfield/have {}'.format(ip, e.args))
                    print('downloader:  {}: error in reading {}'.format(ip, msg_ident))
                    self._close_peer_connection(peer)
                else:
                    logging.debug("downloader: {}: successfully read {}".format(ip, bt_messages[msg_ident]))
                    print("downloader: {}: successfully read {}".format(ip, bt_messages[msg_ident]))
                finally:
                    logging.debug('downloader: {}: finally while waiting for bitfield or Have'.format(ip))
                    if not self.active_peers:
                        # close Task so loop stops and program can reconnect to Tracker 
                        return
                    else:
                        # still more peers to connect to
                        pass
            
        while any(peer.has_pieces for peer in self.active_peers.values() if self.active_peers):
            # start the piece process:
            # interested --> request --> process blocks received --> request ...
        
            # select a piece_index and a peer
            index, ip  = self._get_next_piece()
            peer = self.active_peers[ip]

            # write Interested
            if self.channel_state[ip].open and not self.bt_state[ip].interested:
                peer.writer.write(INTERESTED)
                self.process_write_msg(peer, bt_messages_by_name['Interested'])
                self.reset_keepalive(peer)
                logging.debug("{}: wrote INTERESTED".format(ip))
                print("{}: wrote INTERESTED".format(ip))

            # if Choked, Read Unchoked
            while bt_messages[msg_ident] != 'Unchoke':
                if self.channel_state[ip].open and self.bt_state[ip].choked:
                    try:
                        logging.info("{}: client ready to receive Unchoke".format(ip))
                        print("{}: client ready to receive Unchoke".format(ip))
                        self.send_keepalive(peer)
                        msg_ident = yield from self.read_peer(peer) # read and process
                    except (ProtocolError, TimeoutError, OSError, ConnectionResetError) as e:
                        logging.debug('downloader: {} expected Unchoke {}'.format(ip, e.args))
                        print('downloader: {} expected Unchoke {}'.format(ip, e.args))
                        self._close_peer_connection(peer)
                    except Exception as e:
                        logging.debug('downloader: {} expected Unchoke Other Exception 2 {}'.format(ip, e.args))
                        print('downloader: {} expected Unchoke {}'.format(ip, e.args))
                        self._close_peer_connection(peer)
                    else:
                        logging.debug("downloader: {}: read {}".format(ip, bt_messages[msg_ident]))
                    finally:
                        logging.debug('downloader: {}: finally while waiting for Unchoke'.format(ip))
                        if not self.active_peers:
                            # close Task so loop stops and program can reconnect to Tracker 
                            return
                        else:
                            # still more peers to connect to
                            yield
                else:
                     # channel is closed or unchoked
                     break                
            
            # write Request and read Piece
            while index not in self.pbuffer.completed_pieces:     
                if self.channel_state[ip].open and not self.bt_state[ip].choked:

                    # write Request
                    offset = 0 if index not in self.pbuffer.piece_info else self.pbuffer.piece_info[index]['offset']
                    print('downloader: ready to write Request to {} offset: {}'.format(ip, offset))
                    logging.debug('downloader: ready to write Request to {} offset: {}'.format(ip, offset))
                    msg = self.make_request_msg(index, offset)
                    peer.writer.write(msg)
                    self.process_write_msg(peer, bt_messages_by_name['Request'])
                    logging.debug("{}: wrote Request".format(ip))
                    print("{}: wrote Request".format(ip))
                
                    # read Piece
                
                    try:
                        print('downloader: {} expect to receive Piece'.format(ip))
                        msg_ident = yield from self.read_peer(peer)

                    except (ProtocolError, TimeoutError, OSError, ConnectionResetError) as e:
                        logging.debug('downloader: {} expected Piece {}'.format(ip, e.args))
                        print('downloader: {} expected Piece {}'.format(ip, e.args))
                        self._close_peer_connection(peer)
                    except Exception as e:
                        print(e)
                        logging.debug("downloader: expect to read Piece from ip: {} {} \
                        channel_state: {} \
                        open: {} \
                        choked: {} \
                        interested: {}"\
                            .format(e.args, ip, \
                            self.channel_state[ip].state, \
                            self.channel_state[ip].open,\
                            self.bt_state[ip].choked, \
                            self.bt_state[ip].interested))
                        self._close_peer_connection(peer)
                    else:
                        logging.debug("{}: read {}".format(ip, bt_messages[msg_ident]))
                        print("{}: read {}".format(ip, bt_messages[msg_ident]))
                        while bt_messages[msg_ident] != 'Piece':
                            if self.channel_state[ip].open and not self.bt_state[ip].choked:
                                try:
                                    msg_ident = yield from self.read_peer(peer)

                                except (ProtocolError, ConnectionError, TimeoutError, OSError) as e:
                                    logging.debug('downloader: {} reading for Piece {}'.format(ip, e.args))
                                    print('downloader: {} reading for Piece {}'.format(ip, e.args))
                                    self._close_peer_connection(peer)
                                except Exception as e:
                                    print(e.args)
                                    logging.debug("downloader: reading for Piece  ip: {} \
                                    channel_state: {} \
                                    open: {} \
                                    choked: {} \
                                    interested: {}"\
                                        .format(ip, \
                                        self.channel_state[ip].state, \
                                        self.channel_state[ip].open,\
                                        self.bt_state[ip].choked, \
                                        self.bt_state[ip].interested))
                                    self._close_peer_connection(peer)
                                else:
                                    logging.debug("downloader: {}: read {}".format(ip, bt_messages[msg_ident]))
                                    print("downloader: {}: read {}".format(ip, bt_messages[msg_ident]))
                                finally:
                                    logging.debug('downloader: {}: finally while reading for Piece msg'.format(ip))
                                    if not self.active_peers:
                                        return
                                    else:
                                        yield
                            else:
                                # channel is closed or choked
                                break
                else:
                    # channel is closed or choked
                    break

            if len(self.pbuffer.completed_pieces) == self.torrent.number_pieces:
                # all pieces received
                logging.info("completed pieces: {} torrent pieces: {}"\
                    .format(len(self.pbuffer.completed_pieces, \
                    self.torrent.number_pieces)))
                return  # close task
            else:
                # request more pieces
                pass


    @asyncio.coroutine
    def uploader(self):
        pass

    def downloader_clean_up(self):
        # piece: ips and ip:pieces data structures
        self.piece_to_peers = {}
        self.peer_to_pieces = {}

    def close_quiet_channels(self):
        """
        closes channels that have timed out
        timeout is CHANNEL_TIMEOUT secs
        """
        for peer in self.active_peers.values():
            if datetime.datetime.utcnow() - peer.timer > CHANNEL_TIMEOUT:
                # close channel
                self._close_peer_connection(peer)
                
    def send_keepalive(self, peer):
        """sends keep_alive to peer if timer was updated > CHANNEL_TIMEOUT secs ago"""
        if datetime.datetime.utcnow() - peer._client_keepalive_timer > CHANNEL_TIMEOUT:
            peer.writer.write(KEEPALIVE)
            peer._client_keepalive_timer = datetime.datetime.utcnow()
            logging.debug('wrote KEEPALIVE to {}'.format(peer.address[0]))
            print('wrote KEEPALIVE to {}'.format(peer.address[0]))
          
  
    @asyncio.coroutine
    def connect_to_peer(self, peer):
        """connect to peer address (ipv4, port) 
        
        write handshake to peer
        read handshake from peer"""

        ip, port = peer.address
        logging.info('connect_to_peer {}'.format(ip))
        try:
            reader, writer = yield from asyncio.open_connection(host=ip, port=port)
        except TimeoutError as e:
            logging.debug("connect_to_peer: TimeoutError: {}".format(ip))
            return
        except OSError as e:
            logging.debug("connect_to_peer: OSError: {}".format(ip))
            return
        except ConnectionError as e:
            logging.debug("connect_to_peer: ConnectionError {}: {}".format(e, ip))
            return
        except Exception as e:
            logging.debug("connect_to_peer: Other Exception..{}: {}".format(e, ip))
            return
        else:
            # successful connection to peer
            # write Handshake
            self._open_peer_connection(peer, reader, writer)
            logging.info('connect_to_peer {}: connection open'.format(ip))
            print('connect_to_peer {}: connection open'.format(ip))
                    
    @asyncio.coroutine
    def _read_handshake(self, peer):
        ip, _ = peer.address
        try:
            # read Handshake from peer
            logging.debug('about to readexactly 68 handshake bytes')
            print('about to readexactly 68 handshake bytes from {}'.format(peer.address[0]))
            msg_hs = yield from peer.reader.readexactly(68)      # read handshake msg
        except (ConnectionError, ProtocolError, ConnectionResetError) as e:
            logging.debug('_read_handshake {} Not ConnectionResetError'.format(peer.address[0]))
            print(e)
            self._close_peer_connection(peer)
            return
        except Exception as e:
            logging.debug('_read_handshake {} Other Exception'.format(peer.address[0]))
            print(e)
            self._close_peer_connection(peer)
            return
        else:
            # received Handshake from peer
            msg_ident = self.process_read_msg(peer, msg_hs)
            print('received {} from peer {} channel state: {}'\
                .format(bt_messages[msg_ident], ip, self.channel_state[ip].state))
            return msg_ident
                             
    @asyncio.coroutine
    def read_peer(self, peer):
        """
        reads msg from peer and processes it
        """
        ip, _ = peer.address
        reader = peer.reader

        logging.debug('in read_peer')
        print('in read_peer')
        
        if self.channel_state[ip].open == 1 and self.channel_state[ip].state == 1:
            logging.info('read_peer: read Handshake from {}'.format(ip))
            try: 
                msg = yield from self._read_handshake(peer)

            except Exception as e:
                logging.debug('read_peer {}: read Handshake error {}'.format(ip, e.args))
                raise ProtocolError from Exception
            else:
                logging.info('read_peer: Received Handshake from {}'.format(ip))
                msg_ident = HANDSHAKE_ID
                return msg_ident
        else:
            try:
                msg_length = yield from reader.readexactly(4)

            except (ProtocolError, TimeoutError, OSError) as e:
                logging.debug("read_peer {}: caught error from length: {}".format(ip, e.args))
                raise
            except Exception as e:
                logging.debug("read_peer {}: caught Other Exception 2:  {}".format(ip, e.args))
                #if reader._transport_.conn_lost:
                raise reader.exception()
            else:
                if msg_length == KEEPALIVE:
                    peer.timer = datetime.datetime.utcnow()
                    print('read_peer: received Keep-Alive from peer {}'.format(ip))
                    msg_ident = KEEPALIVE_ID
                else:
                    try:
                        msg_body = yield from reader.readexactly(self._4bytes_to_int(msg_length))

                    except (ProtocolError, TimeoutError, OSError) as e:
                        logging.debug("read_peer {}: caught error from body {}".format(ip, e.args))
                        raise
                    except Exception as e:
                        logging.debug("read_peer {}: caught Other Exception {}".format(ip, e.args))
                        raise reader.exception()
                    else:
                        msg_ident = msg_body[0]
                        if msg_ident in list(range(10)) or msg_ident == KEEPALIVE_ID:
                            self.process_read_msg(peer, msg_length + msg_body)
                            print('read_peer: received {} from peer {} channel state: {}'\
                                .format(bt_messages[msg_ident], \
                                ip, self.channel_state[ip].state))
                            logging.debug('read_peer: received {} from peer {} channel state: {}'\
                                .format(bt_messages[msg_ident], \
                                ip, self.channel_state[ip].state))
                        else:
                            # msg_ident not supported
                            logging.debug("read_peer: msg_id {} not supported \
                            channel state: {}"\
                                .format(msg_ident, self.channel_state[ip].state))
                            msg_ident = NOTSUPPORTED_ID
                return msg_ident

class Peer(object):
    def __init__(self, torrent, address):
        self.torrent = torrent
        self.address = address # (ipv4, port)
        self.peer_id = None
        self.has_pieces = set()  # set of piece indices
        self.reader = None
        self.writer = None
        # bt_state = choked by client, interested in client
        self.bt_state = BTState()
        # peer keepalive timer (reset when client receives msg from peer)
        self._timer = datetime.datetime.utcnow() 
        # client keepalive timer (reset when client sends msg to peer)
        self._client_keepalive_timer = datetime.datetime.utcnow() 


########## 
if __name__ == "__main__":
    torrent_obj = TorrentWrapper("Mozart_mininova.torrent")
    assert torrent_obj.is_multi_file() == True

    client = Client(torrent_obj)
    #client.connect_to_tracker(PORTS[0])
    # print(torrent_obj.list_of_file_boundaries())

    loop = asyncio.get_event_loop()
    loop.set_debug(enabled=True)
    logging.captureWarnings(capture=True)

    #task_downloader = [client.downloader()]
    #task_uploader = [client.uploader()]
    
    port_index = 0
    while len(client.pbuffer.completed_pieces) != client.torrent.number_pieces:
        #logging.debug('size of client buffer: {}'.format(len(client.pbuffer.buffer[0])))
        success = client.connect_to_tracker(PORTS[port_index])  # blocking
        port_index = (port_index + 1) % len(PORTS)  
        
        if success:
            task_downloader = [client.downloader()]
            tasks = [client.connect_to_peer(peer) for peer in client.active_peers.values()]
            try:
                loop.run_until_complete(asyncio.wait(tasks + task_downloader))
            except Exception:
                logging.debug('exception received at top level')
                print('exception received at top level')
                pass
            #loop.close()

    loop.close()
       
print()





        