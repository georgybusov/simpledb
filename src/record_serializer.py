# Function: This component is responsible for converting your structured Python data (e.g., a dictionary representing a row) into a compact, efficient byte string that can be stored on disk, and vice-versa. This is where you decide how different data types (integers, strings, booleans) are encoded.
# Key Responsibilities:
    # serialize(record: dict) -> bytes: Takes a Python dictionary (your "row") and turns it into a byte string. You'll need to decide on a format (e.g., length-prefixed strings, fixed-size integers).
        # Use varints for header size, and serial types
    # deserialize(data: bytes) -> dict: Takes a byte string from disk and reconstructs the Python dictionary.

# Interaction: The RecordStore will use the RecordSerializer whenever it needs to write a record to disk or read one from disk.

import constants
import struct

class RecordSerializer:

    def __init__(self,encoding = 'utf-8'):
        self.encoding = encoding

# I want to mimic record stores in SQLite meaning we have an integer --> type mapping that shows us which datatype a given bytestream should be in

# Serial Type	Content Size	Meaning
# 0	0	Value is a NULL.
# 1	1	Value is an 8-bit twos-complement integer.
# 2	2	Value is a big-endian 16-bit twos-complement integer.
# 3	3	Value is a big-endian 24-bit twos-complement integer.
# 4	4	Value is a big-endian 32-bit twos-complement integer.
# 5	6	Value is a big-endian 48-bit twos-complement integer.
# 6	8	Value is a big-endian 64-bit twos-complement integer.
# 7	8	Value is a big-endian IEEE 754-2008 64-bit floating point number.
# 8	0	Value is the integer 0. (Only available for schema format 4 and higher.)
# 9	0	Value is the integer 1. (Only available for schema format 4 and higher.)
# 10,11	variable	Reserved for internal use. These serial type codes will never appear in a well-formed database file, but they might be used in transient and temporary database files that SQLite sometimes generates for its own use. The meanings of these codes can shift from one release of SQLite to the next.
# N≥12 and even	(N-12)/2	Value is a BLOB that is (N-12)/2 bytes in length.
# N≥13 and odd	(N-13)/2	Value is a string in the text encoding and (N-13)/2 bytes in length. The nul terminator is not stored.

 # In SQLitem, headers and serial record types are encoded into varints which I want to mimic
    @staticmethod
    def encode_varint(n: int) -> bytes:
    # The varint algorithm is to stores header_sizes and serial_records in as little space as possible. It transform any int value into multiple 7 bit chunks (usually just 1 is enough)
    # We transform the given n into bits and then take chunks of 7 bits (we use the 1st bit to show if there are more 7-bit chunks to read or not)
    # taking the number 300 for example: would be 1_0010_1100
    # while the values are greater than 128 (8 bits) we are going to continue using multiple 7-bit chunks to stores them
    # so we take the first 7 bits --> 010_1100, and append it to chunks_list list of 7 bit chunks, every 7-bit chunk gets a digit
    # since there are more bits coming, we add a 1 to the front of the 7 bit --> 1010_1100, is the int=172 --> 0xac (we append this hex to our list)
    # now we shift 7 bits to the left from from the original 1_0010_1100 we are now are here 1_0(here)010_1100 which is the same as 000_0010 as a 7 bit chunk
    # now that the rest of the 7 bit chunks are just going to be 0s, we know we are done and don't have to add a 1 to the front of this 7 bit chunk, and we just have 0000_0010 which is int = 2 --> 0x02 (we append this hex to our list)

        # can we fit the n into a single 7 bit chunk?
        if n < 128:
            # if yes then just return it as bytes
            return bytes([n])
        # otherwise lets start a list to append multiple 7 bit chunks
        chunks_list = []
        # while we need more than one 7 bit chunk to store the n
        while n >= 128:
            # continue appending the 7 bit chunks and adding 128 (which is the same as adding a 1 to a 7 bit chunk since 1000_0000 in bits is 128)
            chunks_list.append((n & 127) | 128)
            # and then move up 7 bits to the left to get the next 7 bits (same as adjusting n by the number of times its divisible by 128)
            n = n//128
        # we can then append this value (max 255--> 8 bits 1111_1111) to chunks list and process the next 7 bits
        chunks_list.append(n)
        return bytes((chunks_list))
    
    @staticmethod
    def decode_varint(data: bytes) -> int:
        # initialize the future n (sum of all bytes)
        result_sum = 0
        # start tracker to see how many 7_bit chunks have been used
        chunk_position = 0

        for i, byte in enumerate(data):
            # get the last 7 bits from the bytes
            value = byte % 128  
            # add it to the future n (result_sum)
            result_sum += value * (128 ** chunk_position)
            # if 8th position bit is 0, we just processed last 7 bit chunk and are done
            if byte < 128:  
                return result_sum, i + 1
            # adjust the 7 bit chunk that you are on
            chunk_position += 1

    # Now that we have the function to encode an int into as few hexadecimals as possible we can start encoding and serializing a record into a bytestream
    def serialize(self, record: dict) -> bytes:
                    
        header_fields = []
        body = []

        for value in record.values():
            if value is None:
                serial_type = constants.SERIAL_RECORD_NULL
                body_bytes = b''

            elif isinstance(value, int):
                serial_type = constants.SERIAL_RECORD_INT
                body_bytes = value.to_bytes(serial_type, 'big', signed=True)

            elif isinstance(value, float):
                serial_type = constants.SERIAL_RECORD_FLOAT
                body_bytes = struct.pack('>d', value)

            elif isinstance(value, str):
                encoded = value.encode(self.encoding)
                serial_type = 13 + len(encoded) * 2  # odd => text
                body_bytes = encoded

            elif isinstance(value, bool):
                value = int(value)
                serial_type = constants.SERIAL_RECORD_INT
                body_bytes = value.to_bytes(serial_type, 'big', signed=True)

            else:
                raise TypeError(f"No support for type: {type(value)} yet")

            header_fields.append(self.encode_varint(serial_type))         
            body.append(body_bytes)

        # Header size = size of all serial type varints + varint(header_size)
        # take header fields and turn them from list to bytestream
        header_bytestream = b''.join(header_fields)
        # Get total header size to encode
        header_size = len(header_bytestream) + 1 #here we are assuming that we won't have more than 128 columns so header_length is always 1 byte long

        full_header = self.encode_varint(header_size) + header_bytestream

        return full_header + b''.join(body)
    
    def deserialize(self,bytestream:bytes, columns: list[str]) -> dict:
        # extract header size and how much to offset the data to find serial types
        header_size, offset = self.decode_varint(bytestream)
        # use cursor
        cursor = offset
        # locate serial_types
        decoded_serial_types = []

        # while cursor is less than header_size
        while cursor < header_size:
            serial_type, n_bytes = self.decode_varint(bytestream[cursor:])
            decoded_serial_types.append(serial_type)
            cursor += n_bytes

        record = {}
        body = bytestream[header_size:]
        body_cursor = 0

        for i,serial_type in enumerate(decoded_serial_types):
            
            if serial_type == constants.SERIAL_RECORD_NULL:
                value = None
            
            # string will always be more than 13 because its 13 + len(encoded) * 2
            elif serial_type >= 13:
                
                # get true length and true value
                value_size = (serial_type - 13) // 2
                byte_value = body[body_cursor:body_cursor+value_size]
                value = byte_value.decode(self.encoding)
                
                body_cursor += value_size

            elif serial_type == constants.SERIAL_RECORD_INT:
                
                # get true length and true value
                value_size = constants.SERIAL_RECORD_INT
                byte_value = body[body_cursor:body_cursor+value_size]
                value = int.from_bytes(byte_value,'big')
                
                body_cursor+=value_size
            
            elif serial_type == constants.SERIAL_RECORD_FLOAT:
                # get true length and true value --> float always has 8 when packed as struct.pack('>d',{float_value})
                value_size = 8
                byte_value = body[body_cursor:body_cursor+value_size]
                value = struct.unpack('>d',byte_value)[0]

                body_cursor +=8

            record[columns[i]] = value

        return record