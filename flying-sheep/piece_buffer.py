
import math
import hashlib
import array
import asyncio
import logging

from bt_utils import BufferFullError
from bt_utils import BLOCK_SIZE

logger = logging.getLogger('asyncio')
logging.basicConfig(filename="bittorrent.log", filemode='w', level=logging.DEBUG, format='%(asctime)s %(message)s')
logging.captureWarnings(capture=True)

class PieceBuffer(object):
    """
    stores the bytes in a piece
    stores up to BUFFER_SIZE pieces
    """
    def __init__(self, torrent):
        self.torrent = torrent
        self.buffer = {row: array.array('B', \
            bytearray(self.torrent.torrent['info']['piece length'])) \
            for row in range(self.torrent.number_pieces + 1)}
        self.free_rows = {i for i in range(self.torrent.number_pieces + 1)}
        self.completed_pieces = set()  # set of completed pieces (indices); store as a client attribute?
        # {'piece index': 20, 'all_bytes_received': False, 'hash_verifies': False, 'bitfield': 1111100000, 'offset': 0x8000}
        self.piece_info = {}
        
    def is_full(self):
        return not self.free_rows
    
    def is_registered(self, piece_index):
        return piece_index in self.piece_info  

    def insert_bytes(self, piece_index, begin, block):
        """
        inserts bytes into the array for piece_index

        piece_index: int
        begin: int
        block: sequence of bytes (bytearray)
        """

        row = self.piece_info[piece_index]['row']
        
        # insert block of bytes
        ablock = array.array('B', block)
        self.buffer[row][begin:begin+len(ablock)] = ablock

        logging.debug('insert_bytes: length(ablock) = {}, {}'.format(len(ablock), ablock[:5]))
        print('insert_bytes: length(block) = {}, {}'.format(len(ablock), ablock[:5]))

        logging.debug('insert_bytes: PieceBuffer buffer[{}]: {} begin: {}'
            .format(row, self.buffer[row][begin+len(ablock)-5:begin+len(ablock)], begin))
        print('insert_bytes: PieceBuffer buffer[{}]: {} begin: {}'
            .format(row, self.buffer[row][begin+len(ablock)-5:begin+len(ablock)], begin))

        # update bitfield (each bit represents a block in the piece)
        #self._update_bitfield(piece_index)
        self._update_bitfield(piece_index, begin)

        # check if all blocks received
        if self._is_all_blocks_received(piece_index):
            
            if self._is_piece_hash_good(piece_index):
                # all bytes received
                self.piece_info[piece_index]['all_blocks_received'] = True
                # piece hash matches torrent hash
                print('all_blocks_received: hash verifies for piece index {}'.format(piece_index))
                logging.debug('all_blocks_received: hash verifies for piece index {}'.format(piece_index))
                self.piece_info[piece_index]['hash_verifies'] = True
                self.completed_pieces.add(piece_index)  # set of piece indices
                return 'done'
            else:
                # piece hash does not match torrent match
                return 'bad hash'
        else:
            # not all blocks received
            return 'not done'

    def reset(self, piece_index, free_row=False):
        """
        reset buffer row to bytearray()
        reset bitfield to all 0s
        if free_row == False: 
            reset row but keep row in buffer
        if free_row == True: 
            add row to set of available rows
            row data is no longer in buffer
        """     
        row = self.piece_info[piece_index]['row']

        self.buffer[row] = array.array('B', bytearray(self.torrent.piece_length))
        self.piece_info[piece_index]['bitfield'] = self._init_bitfield(piece_index)
        self.piece_info[piece_index]['offset'] = 0
        
        if free_row:
            del self.piece_info[piece_index]
            self.free_rows.add(row)  # add row to set of available rows

    def pieces_in_buffer(self):
        """returns an iterator object that iterates over pieces in buffer"""
        piece_indices = list(self.piece_info.keys())
        return iter(piece_indices)

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
            print("Buffer is full")
            raise BufferFullError("Buffer is full")
        else:
            self.piece_info[piece_index] = {'row': row,
                                    'all_blocks_received': False,
                                    'hash_verifies': False,
                                    'bitfield': self._init_bitfield(piece_index),
                                    'offset': 0
                                    }
            #logging.debug('_register_piece: self.free_rows: {} index {}'.format(self.free_rows, piece_index))
            #print('_register_piece: self.free_rows: {} index {}'.format(self.free_rows, piece_index))

    def _sha1_hash(self, piece_index):
        """hash the piece bytes"""
        sha1 = hashlib.sha1()
        try:
            row = self.piece_info[piece_index]['row']
        except KeyError:
            print("piece index {} is not in buffer".format(piece_index))
            logging.debug("piece index {} is not in buffer".format(piece_index))
            raise KeyError

        if piece_index != self.torrent.LAST_PIECE_INDEX:
            piece = self.buffer[row]
        else:
            piece = self.buffer[row][:self.torrent.last_piece_length]

        print('_sha1_hash: piece_index: {} bytes: {} {}'\
            .format(piece_index, piece[:10], piece[:-5:-1]))
        logging.debug('_sha1_hash: piece_index: {} bytes: {} {}'\
            .format(piece_index, piece[:10], piece[:-5:-1]))
        sha1.update(piece)
        return sha1.digest()

    def _is_piece_hash_good(self, piece_index):
        torrent_hash_value = self.torrent.get_hash(piece_index)
        piece_hash_value = self._sha1_hash(piece_index)
        print(torrent_hash_value[:10])
        print(piece_hash_value[:10])
        return torrent_hash_value == piece_hash_value
       
    def _is_all_blocks_received(self, piece_index):
        """
        returns True if bitfield has no "0"
        """
        bf = bin(self.piece_info[piece_index]['bitfield'])[3:]
        print('_is_all_blocks_received: blocks bitfield: {}'.format(bf))
        logging.debug('_is_all_blocks_received: blocks bitfield: {}'.format(bf))
        return not('0' in bf)

    def _init_bitfield(self, piece_index):
        """
        init bitfield for a buffer row 
        all bits initialized to 0
        rightmost bit (LSB): block 0
        """
        try:
            if piece_index != self.torrent.LAST_PIECE_INDEX:
                number_of_blocks = math.ceil(self.torrent.piece_length / BLOCK_SIZE) 
            else:
                number_of_blocks = math.ceil(self.torrent.last_piece_length / BLOCK_SIZE)
        except Exception as e:
            print('pbuffer._init_bitfield: {}'.format(e.args))
            logging.debug('pbuffer._init_bitfield: {}'.format(e.args))
            raise e 
             
        field = 1 << number_of_blocks
        return field
    
    def _update_bitfield(self, piece_index, offset):
        """
        update bitfield for buffer row
        each bit represents a block
        rightmost bit (LSB): block 0
        """
       
        block_number = offset // BLOCK_SIZE

        bfs = bin(self.piece_info[piece_index]['bitfield'])[3:]
        length = len(bfs)
        logging.debug('_update_bitfield (pbuffer): {} block number: {}'\
            .format(bfs[length-1-block_number], block_number))
        print('_update_bitfield (pbuffer): {} block number: {}'\
            .format(bfs[length-1-block_number], block_number))

        self.piece_info[piece_index]['bitfield'] |= 1 << block_number

        bfs = bin(self.piece_info[piece_index]['bitfield'])[3:]
        length = len(bfs)
        logging.debug('_update_bitfield (pbuffer): {}'.format(bfs[length-1-block_number]))
        print('_update_bitfield (pbuffer): {}'.format(bfs[length-1-block_number]))