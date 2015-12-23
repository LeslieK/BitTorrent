"""
This module provides utility functions for a bit torrent client.
"""
import os
import math
import datetime
import hashlib
import socket
from bcoding import bdecode, bencode

ROOT_DIR = 'c:\\Users\\lbklein\\PROJECTS\\VisualStudio2015Projects\\AsynchIO\\flying-sheep'

HASH_DIGEST_SIZE = 20  # in bytes
PEER_ID_SIZE = 20 # in bytes

DEFAULT_BLOCK_LENGTH = 2**14
BLOCK_SIZE = DEFAULT_BLOCK_LENGTH
MAX_BLOCK_SIZE = bytes([4, 0, 0, 0]) # 2**14 == 16384
MAX_PEERS = 50
MAX_PIECES_TO_REQUEST = 1  # used in client._get_next_piece(); number of pieces to request at once
MAX_UNCHOKED_PEERS = 5

NUMWANT = 10  # GET parameter to tracker
CONNECTION_TIMEOUT = datetime.timedelta(seconds=90)
HANDSHAKE_ID = 100
KEEPALIVE_ID = 200
NOTSUPPORTED_ID = 300

PORTS = list(range(6881, 6890))
HANDSHAKE_ID = 100
KEEPALIVE_ID = 200
NOTSUPPORTED_ID = 300

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

class BTState:
    def __init__(self, choked=1, interested=0):
        self.choked = choked
        self.interested = interested

class ChannelState:
    def __init__(self, open=0, state=0):
        self.open = open
        self.state = state

class ConnectionError(Exception):
    pass

class ConnectionResetError(Exception):
    pass

class BufferFullError(Exception):
    pass

class ProtocolError(Exception):
    pass

class HashError(Exception):
    pass

def sha1_info(torrent):
    """returns sha1 hash of torrent['info']"""
    sha1 = hashlib.sha1()
    info_bytes = bencode(torrent['info'])
    sha1.update(info_bytes)
    return sha1.digest()

def my_peer_id(size=PEER_ID_SIZE):
    return os.urandom(size)

def make_handshake(info_hash, peer_id):
    """
    info_hash: self.torrent.INFO_HASH
    peer_id: client's unique id (from my_peer_id())
    """

    pstrlen = b'\x13'  # 19
    pstr = b'BitTorrent protocol'
    reserved = b'00000000'
    return pstrlen + pstr + reserved + info_hash + peer_id

def rcv_handshake(buf):
    """
    parse handshake msg
    buf: bytearray(msg)

    input: complete msg received from peer
    output: msg dict
    """
    msgd = {}
    pstrlen = buf[0]
    if pstrlen != 19:
        raise ValueError('Expected pstrlen is 19. Received pstrlen {}'.format(pstrlen))
    msgd['pstrlen'] = pstrlen
    try:
        pstr = buf[1:pstrlen + 1].decode('utf-8')
    except UnicodeDecodeError:
        raise ValueError("Expected 'BitTorrent protocol'. Received {}".format(buf[1:pstrlen + 1]))
    else:
        msgd['pstr'] = pstr
        msgd['reserved'] = buf[pstrlen+1:pstrlen+9]
        msgd['info_hash'] = buf[pstrlen+9:pstrlen+29]
        msgd['peer_id'] = buf[pstrlen+29:pstrlen+49]
    return msgd

def number_of_blocks(piece_index, torrent):
    if piece_index != torrent.LAST_PIECE_INDEX:
        return math.ceil(torrent.piece_length / BLOCK_SIZE) 
    else:
        return math.ceil(torrent.last_piece_length / BLOCK_SIZE)




