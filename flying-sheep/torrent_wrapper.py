
from bcoding import bdecode
#from collections import defaultdict, Counter
import math
import os
import asyncio
import logging

from bt_utils import sha1_info # my_peer_id
from bt_utils import HASH_DIGEST_SIZE

# create logger for torrent_wrapper module
module_logger = logging.getLogger(__name__)
#logger.setLevel(logging.DEBUG)

## create FileHandler which logs debug messages
#fh = logging.FileHandler('main_bt.log')
#fh.setLevel(logging.INFO)

## create console handler with a higher log level
#ch = logging.StreamHandler()
#ch.setLevel(logging.INFO)

## create formatter and add it to the handlers
#formatter = logging.Formatter('%asctime)s - %(name)s - %(levelname)s - %(message)s')
#fh.setFormatter(formatter)
#ch.setFormatter(formatter)

## add the handlers to the logger
#logger.addHandler(fh)
#logger.addHandler(ch)

#logging.basicConfig(filename="bittorrent.log", filemode='w', level=logging.DEBUG, format='%(asctime)s %(message)s')
#logging.captureWarnings(capture=True)

class TorrentWrapper(object):
    """
    for a multi-file torrent, the piece indices are in the order of the files in the 'files' element.
    For ex, if 1st file in the 'files' list has 6 pieces and the 2nd file in the 'files' list has 5 pieces, 
    the first 6 of the 11 pieces are for file 1. The next 5 pieces are for file 2.
    """
    def __init__(self, metafile):
        self.logger = logging.getLogger('main_bt.torrent_wrapper.TorrentWrapper')
        self.logger.info('creating a TorrentWrapper instance')
        with open(metafile, 'rb') as f:
            self.torrent = bdecode(f)

        self.INFO_HASH = sha1_info(self.torrent)
        self.TRACKER_URL = self.announce()
        self.piece_length = self.torrent['info']['piece length']
        self.total_bytes = self.total_file_length()
        
        self.number_pieces = self._num_pieces()
        self.last_piece_length = self._length_of_last_piece()
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
        return math.ceil(self.total_bytes / self.piece_length)

    def _length_of_last_piece(self):
        return self.total_bytes - self.piece_length * (self.number_pieces - 1)

    def total_file_length(self):
        """returns the number of bytes across all files"""
        if self.is_multi_file():
            return sum([file['length'] for file in self.torrent['info']['files']])
        else:
            # single file
            return self.torrent['info']['length']

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