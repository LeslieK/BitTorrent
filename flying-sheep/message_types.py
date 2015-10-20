﻿"""
This module defines the message types of the BitTorrent protocol.
"""
from bcoding import bdecode, bencode
from collections import defaultdict, Counter
import math
import hashlib
import array
import bisect
from datetime import datetime  # timezone
import sys, os, socket
import requests
import asyncio

from bt_utils import my_peer_id, sha1_info, make_handshake
from bt_utils import rcv_handshake

from bt_utils import HASH_DIGEST_SIZE

DEFAULT_BLOCK_LENGTH = 2**14
MAX_PEERS = 50
MAX_BLOCK_SIZE = b'\x04\x00\x00\x00' # 2**14 == 16384
NUMWANT = 10  # GET parameter to tracker
BUFFER_SIZE = 5
CHANNEL_TIMEOUT = datetime.timedelta(seconds=90)
#HANDSHAKE_ID = 100
#KEEPALIVE_ID = 200

PORTS = [i for i in range(6881, 6890)]
events = ['started', 'stopped', 'completed']

KEEPALIVE = b'\x00\x00\x00\x00'
CHOKE = b'\x00\x00\x00\x01' + b'\x00'
UNCHOKE = b'\x00\x00\x00\x01' + b'\x01'
INTERESTED = b'\x00\x00\x00\x01' + b'\x02'
NOT_INTERESTED = b'\x00\x00\x00\x01' + b'\x03'

class ConnectionError(Exception):
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

#bt_messages = {'choke': 
#               {'length': 1, 'id': 0,},
#               'unchoke':
#               {'length': 1, 'id': 1},
#               'interested':
#               {'length': 1, 'id': 2},
#               'not interested':
#               {'length': 1, 'id': 3},
#               'have':
#               {'length': 5, 'id': 4, 'payload': {'index': 0}},
#               'bitfield':
#               {'length': 1, 'id': 5, 'payload': {'bitfield': 0}},
#               'request':
#               {'length': 13, 'id': 6, 'payload': {'index': 0, 'begin': 0, 'length': 0}},
#               'piece':
#               {'length': 9, 'id': 7, 'payload': {'index': 0, 'begin': 0, 'block': 0}},
#               'cancel':
#               {'length': 13, 'id': 8, 'payload': {'index': 0, 'begin': 0, 'length': 0}},
#               'port':
#               {'length': 3, 'id': 9, 'payload': {'listen-port': 0}}
#               }

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
               20: 'Extended'}

bt_messages_by_name = {name: ident for ident, name in bt_messages.items()}

class Piece_Buffer(object):
    """
    stores the bytes in a piece
    stores up to BUFFER_SIZE pieces
    """
    def __init__(self, torrent):
        self.torrent = torrent
        self.buffer ={row: array.array('B', bytearray(self.torrent.torrent['info']['piece length'])) for row in range(BUFFER_SIZE)}
        self.free_rows = {i for i in range(BUFFER_SIZE)}
        self.piece_info = {}
        self.completed_pieces = set()  # set of completed pieces (indices); store as a client attribute?


    def is_full(self):
        return len(self.free_rows) == 0  

    def insert_bytes(self, piece_index, begin, block):
        """
        inserts bytes into the array for piece_index

        piece_index: int
        begin: int
        block: sequence of bytes
        """
        try:
            row = self.piece_info[piece_index]['row']
        except KeyError:
            print("{} not in buffer. Registering piece...".format(piece_index))
            self._register_piece(piece_index)
            return self.insert_bytes(piece_index, begin, block)
        else:
            # insert block of bytes
            self.buffer[row][begin:begin+len(block)] = block
            # update bitfield (each bit represents a byte in the piece)
            self._update_bitfield(begin, length(block))
            # check if all bytes received
            if self._is_all_bytes_received(piece_index):
                self.piece_info[piece_index]['all_bytes_received'] = True
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
        self.buffer[row][:] = bytearray(self.torrent['info']['piece length'])
        self.piece_info[piece_index]['bitfield'] = self._init_bitfield()
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
            self.piece_info[piece_index] = {'row': row,
                                            'all_bytes_received': False,
                                            'hash_verifies': False,
                                            'bitfield': self._init_bitfield()
                                           }
        except KeyError:
            print("Buffer is full")
            raise BufferFullError

    def _sha1_hash(self, piece_index):
        """hash the piece bytes"""
        try:
            row = self.piece_info[piece_index]
        except KeyError:
            print("piece index {} is not in buffer".format(piece_index))
            raise KeyError
        else:
            sha1 = hashlib.sha1()
            if piece_index != self.LAST_PIECE_INDEX:
                return sha1.update(self.buffer[row]).digest()
            else:
                return sha1.update(self.buffer[row][:self.last_piece_length]).digest()

    def _is_piece_hash_good(self, piece_index):
        torrent_hash_value = self.torrent.get_hash(piece_index)
        piece_hash_value = self._sha1_hash(piece_index)
        return torrent_hash_value == piece_hash_value
       
    def _is_all_bytes_received(self, piece_index):
        bf = bin(self.piece_info[piece_index]['bitfield'])[3:]
        return all(s)

    def _update_bitfield(self, begin, length):
        # this needs to be fixed for performance
        i = begin
        while i < begin + length:
            self.piece_info[piece_index]['bitfield'] |= (1 << i)
            i += 1

    def _init_bitfield(self):
        """init bitfield for buffer row; indicates which bytes of a piece have been received"""
        return 1 << self.torrent.piece_length if piece_index != self.torrent.number_pieces - 1\
                                              else 1 << self.torrent.last_piece_length

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
        self.bitfield = None
        self.piece_length = self.torrent.torrent['info']['piece length']
        self.last_piece_length = self._length_of_last_piece()
        self.buffer = Piece_Buffer(torrent)
        self.output_fds = self.open_files()
        self.HANDSHAKE = make_handshake(self.torrent.INFO_HASH, self.peer_id)
        self.active_peers = None
        self.bt_state = None
        self.channel_state = None
        self.piece_to_peers = defaultdict(set)
        self.peer_to_pieces = defaultdict(set)
        self.timer = datetime.utcnow()

        
    def connect_to_tracker(self, port, numwant=NUMWANT):
        """parses self.tracker_response
        
        sets active peers (peer is identified by addr)
        sets number of active peers"""

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

        r = requests.get(self.torrent.TRACKER_URL, params=http_get_params)
        tracker_resp = r.content
        self.tracker_response = bdecode(tracker_resp)
        if 'failure reason' in self.tracker_response:
            self.message = self.tracker_response['failure reason']
        elif 'warning message' in self.tracker_response:
            self.message  = self.tracker_response['warning message']
        else:
            self._list_of_peers()  # sets active_peers, bt_state, channel_state
            self.num_peers = len(self.active_peers)
            return True
        return False

    def _close_peer_connection(self, peer):
        """clean-up after failed connection to peer"""
        ip, _ = peer.address
        # close channel state
        self.channel_state[ip].open = 0
        # remove ip from data structures
        if ip in self.peer_to_pieces:
            self._remove_ip_piece_peer_maps(ip)
        # close connection
        peer.reader.set_exception(e)
        peer.writer.set_exception(e)

    def _open_peer_connection(self, peer, reader, writer):
        ip, _ = peer.address
        peer.reader = reader
        peer.writer = writer
        self.channel_state[ip].open = 1
        self.channel_state[ip].state = 1
        peer.writer.write(self.HANDSHAKE)    

    def _list_of_peers(self):
        """
        tracker response['peers']: (ip, port) formated in compact format

        input: byte string of peer addresses (6 bytes per address); tracker_response['peers']
        output: side affects: sets instance variables
        self.active_peers: {ip: Peer(self.torrent, (ip, port)), ...}
        self.bt_state: {ip: BTState() for ip in dict_peers}
        self.channel_state: {ip: ChannelState() for ip in dict_peers}
        """
        peers = self.tracker_response['peers']
        #return [{'ip': socket.inet_ntoa(peers[index*4:(index + 1)*4]), 'port': port_bytes[0]*256 + port_bytes[1]} 
        #                    for index in range(len(peers)//6) 
        #                    for port_bytes in [peers[(index + 1)*4:(index + 1)*4 + 2]]]
        dict_of_peers = {ip: Peer(self.torrent, (ip, port_bytes[0]*256 + port_bytes[1]))
                for index in range(len(peers)//6)
                for port_bytes in [peers[(index + 1)*4:(index + 1)*4 + 2]]
                for ip in [socket.inet_ntoa(peers[index*4:(index + 1)*4])]}
        self.bt_state = {ip: BTState() for ip in dict_of_peers}
        self.channel_state = {ip: ChannelState() for ip in dict_of_peers}
        self.active_peers = dict_of_peers

    def _helper_comp(self, an_index):
        """returns the complement of an_index"""
        num_bytes=math.ceil(self.torrent.number_pieces / 8)
        max_i = num_bytes * 8 - 1
        return max_i - an_index

    def _remove_ip_piece_peer_maps(self, ip):
        """
        removes the ip from the maps:
        self.peer_to_pieces
        self.piece_to_peers
        (map stores 'ip address' not peer object)
        """
        if ip in self.peer_to_pieces:
            for pindex in self.peer_to_pieces[ip]:
                self.piece_to_peers[pindex].remove(ip)
            del self.peer_to_pieces[ip]

    def _remove_index_piece_peer_maps(self, index):
        """
        removes the piece_index from the maps:
        self.piece_to_peers
        self.peer_to_pieces
        (map stores 'ip address' not peer object)
        """
        for ip in self.piece_to_peers[index]:
            self.peer_to_pieces[ip].remove(index)
        del self.piece_to_peers[index]

    def _length_of_last_piece(self):
        return self.total_files_length - self.piece_length * (self.torrent.number_pieces - 1)

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
        row = self.buffer.piece_info[piece_index]
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
        self.buffer.reset(piece_index, free_row=True)


    def _write(self, fd, offset, num_bytes, row):
        fd.write(self.buffer[row][start:start+num_bytes])

    def _close_fds(self):
        [fd.close() for fd in self.output_fds]
        return

    def init_bitfield(self, list_of_pieces):
        """initialize bitfield; each bit represents a piece of torrent"""
        num_bytes = math.ceil(self.torrent.number_pieces / 8)
        field = 1 << (8 * num_bytes)
        for index in map(_helper_comp, list_of_pieces):
            field |= (1 << index)
        self.bitfield = field

    def update_bitfield(self, list_of_pieces):
        """update bitfield with new pieces"""
        for index in map(_helper_comp, list_of_pieces):
            self.bitfield |= (1 << index)

    def select_piece_index(self):
        """select index based on most rare piece first"""
        return 4

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
        length = bytes([len(client.bitfield) + 1])
        ident = b'5'
        return length + ident + client.bitfield

    def get_indices(self, bitfield):
        b = bytearray(bitfield)
        #b = ''.join([x[2:] for x in map(bin, b)])
        b = ''.join([bin(x) for x in b])[2:]
        return {i for i, x in enumerate(b) if x == '1'}

    def make_have_msg(self, piece_index):
        """
        client makes msg after it downloads piece_index and it verifies hash
        """
        length = b'0005'
        ident = b'4'
        return length + ident + self._int_to_4bytes(piece_index)

    def make_cancel_msg(self, request_msg):
        buf = bytearray(request_msg)
        buf[4] = 8  # bytearray requires int values
        return bytes(buf)

    def make_request_msg(self, piece_index, begin_offset, block_len=DEFAULT_BLOCK_LENGTH):
        """only send request to a peer that has piece_index"""

        length = b'000\x0d' # 13
        ident = b'6'
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
        2. peer.has_pieces is a list of piece indices
        """ 
        while True:
            if self.piece_to_peers:
                freq_of_pieces = \
                    Counter({piece: len(ips) 
                             for piece, ips in self.piece_to_peers.items()})\
                                 .most_common()
            else:
                # no pieces left
                return
            # choose least frequent piece
            piece_index, cnt = freq_of_pieces[-1]
            # the choice of the ip can be random.choice(list(range(cnt)))         
            ip = self.piece_to_peers[piece_index][0]  # returns (piece_index, ip)
            yield piece_index, ip

    def _filter_on_not_complete():
            return {pindex: ips for pindex, ips in self.piece_to_peers.items() 
                    if pindex not in self.buffer.completed_pieces}

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
        self.buffer.insert_bytes(index, begin, block)
        if self.buffer.piece_info[index]['hash_verifies']:
            self.num_bytes_downloaded += len(block)
            self.num_bytes_left -= self.num_bytes_downloaded
            self.update_bitfield([index])
            # update data structures that store piece:{ips} and peer:{pieces}
            self._remove_index_piece_peer_maps(index) 
            # make/send have msg for this piece; send to ips that don't have it
            pass
        elif buffer.is_piece_complete(index):
            # all bytes received and hash doesn't verify
            self.buffer.reset(index)
            # make/send request msg for this piece
            pass
        else:
            # not all bytes received
            pass

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
                    peer.timer = datetime.utcnow()
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
                peer.timer = datetime.utcnow()
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
                peer.timer = datetime.utcnow()
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
                peer.timer = datetime.utcnow()
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
                peer.timer = datetime.utcnow()
                peer.bt_state.interested = 0  # peer is not interested in client
        elif ident == 4:
            #  have message
            try:
                assert self.channel_state[ip].state != 1
            except AssertionError:
                raise ProtocolError("cannot receive Have msg in state 1")
            else:
                peer.timer = datetime.utcnow()
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
                peer.timer = datetime.utcnow()
                msgd = self.rcv_bitfield_msg(ip, buf)  # set peer's bitfield
                self.channel_state[ip].state = 4
        elif ident == 6:
            #  request message
            peer.timer = datetime.utcnow()
            pass
        elif ident == 7:
            #  piece message
            peer.timer = datetime.utcnow()
            rcv_piece_msg(msg)
            pass
        elif ident == 8:
            #  cancel message
            peer.timer = datetime.utcnow()
            pass
        elif ident == 9:
            #  port message
            peer.timer = datetime.utcnow()
            pass
        elif ident == 20:
            # extension
            peer.timer = datetime.utcnow()
            print('extensions not supported')
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

    @asyncio.coroutine    
    def controller(self):
        for index, ip in self._get_next_piece():
            peer = self.active_peers[ip]
            peer.writer.write(INTERESTED)
            self.process_write_msg(peer, bt_messages_by_name['Interested'])
            try:
                msg = yield from self._read_msg(peer.reader, ip)
            except Exception as e:
                print(e)
                self._close_peer_connection(peer)
                continue
            else:
                # process read msg
                try:
                    msg_ident = self.process_read_msg(peer, msg)
                    if msg_ident is KEEPALIVE:
                        # reset timer
                        pass
            

    @asyncio.coroutine
    def _read_msg(self, peer):
        """
        reader: StreamReader instance for this connection
        output: a discreet msg received from peer
        """
        ip, _ = peer.address
        reader = peer.reader
        while True:
            try:
                msg_length = yield from reader.readexactly(4)
            except ConnectionResetError as e:
                print(e)
                raise ConnectionError
            except Exception as e:
                print(e)
                raise e
            else:
                if msg_length == KEEPALIVE:
                    peer.timer = datetime.utcnow()
                    print('received Keep-Alive from peer {}'.format(ip))
                    return KEEPALIVE
                msg_body = yield from reader.readexactly(self._4bytes_to_int(msg_length))
                msg_ident = msg_body[0]
            if msg_ident in list(range(10)):
                return msg_length + msg_body
            else:
                # msg_ident not supported
                continue
    
    @asyncio.coroutine
    def connect_to_peer(self, peer):
        """connect to peer address (ipv4, port) """

        ip, port = peer.address
        try:
            reader, writer = yield from asyncio.open_connection(host=ip, port=port)
        except (TimeoutError, ConnectionResetError, OSError) as e:
            print(e)
            return
        except Exception as e:
            print(e)
            return
        else:
            # successful connection to peer
            # write Handshake
            self._open_peer_connection(peer, reader, writer)
            print('wrote Handshake to peer {} channel state: {}'\
                .format(ip, self.channel_state[ip].state))
            try:
                msg_hs = yield from peer.reader.readexactly(68)      # read handshake msg
                msg_ident = self.process_read_msg(peer, msg_hs)
                print('received Handshake from peer {} channel state: {}'\
                    .format(ip, self.channel_state[ip].state))
            except (ConnectionError, ProtocolError, ConnectionResetError) as e:
                print(e)
                self._close_peer_connection(peer)
                return
            else:
                while True:
                    try:
                        msg = yield from self._read_msg(peer)
                    except ConnectionResetError as e:
                        print(e)
                        self._close_peer_connection(peer)
                        return
                    else:
                        try:
                            # process input msg
                            if msg is not KEEPALIVE:
                                msg_ident = self.process_read_msg(peer, msg)
                        except ProtocolError as e:
                            print(e)
                            self._close_peer_connection(peer)
                            return
                        else:
                            print('received {} from peer {} channel state: {}'\
                                  .format(bt_messages[msg_ident], 
                                  ip, self.channel_state[ip].state))
                            # return control to client
                            yield

                            


class Peer(object):
    def __init__(self, torrent, address):
        self.torrent = torrent
        self.address = address # (ipv4, port)
        self.peer_id = None
        self.has_pieces = set()  # set of piece indices
        self.reader = None
        self.writer = None
        self.bt_state = BTState()
        self.timer = datetime.utcnow()


########## 
if __name__ == "__main__":
    torrent_obj = TorrentWrapper("Mozart_mininova.torrent")
    assert torrent_obj.is_multi_file() == True

    client = Client(torrent_obj)
    client.connect_to_tracker(PORTS[0])
    # print(torrent_obj.list_of_file_boundaries())

    loop = asyncio.get_event_loop()
    tasks = [client.connect_to_peer(peer) for peer in client.active_peers.values()]
    loop.run_until_complete(asyncio.wait(tasks))
    #for peer in client.active_peers.values():
    #    loop.run_until_complete(client.connect_to_peer(peer))   
print()





        