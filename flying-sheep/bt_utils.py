"""
This module provides utility functions for a bit torrent client.
"""
import os
import math
import hashlib
import socket
from bcoding import bdecode, bencode

HASH_DIGEST_SIZE = 20  # in bytes
PEER_ID_SIZE = 20 # in bytes

def sha1_info(torrent):
    """returns sha1 hash of torrent['info']"""
    sha1 = hashlib.sha1()
    info_bytes = bencode(torrent['info'])
    sha1.update(info_bytes)
    return sha1.digest()

def my_peer_id(size=PEER_ID_SIZE):
    return os.urandom(size)

def make_handshake(info_hash, peer_id):
    """torrent: a TorrentWrapper object"""

    pstrlen = b'\x13'  # 19
    pstr = b'BitTorrent protocol'
    reserved = b'00000000'
    return pstrlen + pstr + reserved + info_hash + peer_id

def rcv_handshake(buf):
    """
    parse handshake msg
    msg is a bytearray

    input: complete msg received from peer
    output: msg dict
    """
    # buf = bytearray(msg)
    msgd = {}
    pstrlen = buf[0]
    if pstrlen != 19:
        raise ValueError('Expected pstrlen is 19. Received pstrlen {}'.format(pstrlen))
    msgd['pstrlen'] = pstrlen
    try:
        pstr = buf[1:pstrlen + 1].decode()
    except UnicodeDecodeError:
        raise ValueError("Expected 'BitTorrent protocol'. Received {}".format(buf[1:pstrlen + 1]))
    else:
        msgd['pstr'] = pstr
        msgd['reserved'] = buf[pstrlen+1:pstrlen+9]
        msgd['info_hash'] = buf[pstrlen+9:pstrlen+29]
        msgd['peer_id'] = buf[pstrlen+29:pstrlen+49]
    return msgd



#def announce(torrent):
#    """
#    torrent: the dict in the .torrent file
#    returns announce url: url of tracker
#    """
#    return torrent['announce']


#def get_hash(torrent, index, digest_size=HASH_DIGEST_SIZE):
#    # hashes = io.BytesIO(torrent['info']['pieces']).getbuffer().tobytes()
#    hashes = torrent['info']['pieces']
#    hash_index = index * digest_size
#    return hashes[hash_index:hash_index + digest_size]

#def num_pieces(torrent, digest_size=HASH_DIGEST_SIZE):
#    return math.ceil(total_file_length(torrent) / torrent['info']['piece length'])

#def total_file_length(torrent):
#    return sum([file['length'] for file in file_info(torrent)])

#def is_multi_file(torrent):
#    return 'files' in torrent['info']

#def file_info(torrent):
#    """
#    if multi-file:
#    returns [{'length': len1, 'name': dir1/dir2/fileA]}, {'length': len2, 'name': 'fileB'}, ... ]
#    if single-file:
#    return [{'length': nn, 'name': filename}]
#    """
#    if is_multi_file(torrent):
#        dict = {}
#        files = torrent['info']['files']
#        file_info = [{'length': file['length'], 'name': os.path.join(*file['path'])} for file in files]      
#    else:
#        name = torrent['info']['name']
#        file_size = torrent['info']['length']
#        file_info = [{'length': torrent['info']['length'], 'name': torrent['info']['name']}]
#    return file_info




## process Tracker Responses
#def tracker_response(r):
#    """
#    input: Tracker Get Response
#    output: bdecoded response
#    """
#    return bdecode(r.content)

#def list_of_peers(peers):
#    """
#    input: byte string of peer addresses (6 bytes per address); tracker_response['peers']
#    output: list of peer dicts [{'ip': <ipv4 addr>, 'port': <port num>}, ... ]
#    """
#    return [{'ip': socket.inet_ntoa(peers[index*4:(index + 1)*4]), 'port': port_bytes[0]*256 + port_bytes[1]} 
#            for index in range(len(peers)//6) 
#            for port_bytes in [peers[(index + 1)*4:(index + 1)*4 + 2]]]        



