from constants import PAGE_SIZE
from enum import Enum
import struct

# Enum to distinguish between leaf and internal pages
class PageType(Enum):
    LEAF = 0
    INTERNAL = 1

class Page:
    def __init__(self, page_id: int, max_size: int = PAGE_SIZE, data: bytes = None, page_type: PageType = PageType.LEAF):
        #page instance is for storing values 
        self.values = [] 
        #all page instances need to be identically sized
        self.max_size = max_size 
        # identify a given page instance
        self.page_id = page_id 
        #keeping track of capacity of the page instnace
        self.current_size = 0 
        # keep track of deleted slots (disabling fragmentation)
        self.deleted_indices = set()
        # keep track if page has been used or not
        self.dirty = False 
        # track if this is a leaf page (holds actual data) or internal page (holds child pointers)
        self.page_type = page_type

        if self.page_type == PageType.LEAF:
            # for storing record values
            self.values = []  
        else:
            # for storing (key, child_page_id) pairs
            self.entries = []  

        # Give it option to pass data into fresh instance
        if data:
            self._load_from_bytes(data)

    def has_space(self, value: bytes | tuple) -> bool:
        #track how much memory will we need to add value to a page
        if self.page_type == PageType.LEAF:
            value_memory_usage = len(value) + 4
        else:
            key, _ = value
            # 2-byte key length + key + 4-byte child pointer
            value_memory_usage = 2 + len(key) + 4  
        return self.current_size + value_memory_usage <= self.max_size


    def add_value(self, value: bytes | tuple) -> int:
        # flag the page as modified
        self.dirty = True

        # LEAF flow
        if self.page_type == PageType.LEAF:
            value_memory_usage = len(value) + 4
            if not self.has_space(value):
                raise ValueError("Value too big for simpledb leaf page.")
            
            #if there are deleted indices lets reuse last deleted page_id
            if self.deleted_indices:
                row_id = self.deleted_indices.pop()
                #write into the row_id you identified
                self.values[row_id] = value
            else:
                # otherwise append it to the values list
                self.values.append(value)
                # assign row_id with value position
                row_id = len(self.values) - 1

            self.current_size += value_memory_usage
            return row_id   
            
        elif self.page_type == PageType.INTERNAL:
            # value is a tuple: (key, child_page_id)
            key, child_pid = value
            entry_bytes = 4 + len(key)  # rough estimate
            self.entries.append((key, child_pid))
            self.current_size += entry_bytes
            return len(self.entries) - 1

    #get value from the page given it's row id
    def get_value(self, row_id: int) -> bytes | tuple:
        
        # LEAF flow
        if self.page_type == PageType.LEAF:
            #Check if this row id is even available (not deleted, not out of index)
            if row_id >= 0 and row_id < len(self.values) and row_id not in self.deleted_indices:
                return self.values[row_id]
            else:
                raise IndexError(f"Row id {row_id} not in {self.page_id} or deleted")
        else:
            if row_id >= 0 and row_id < len(self.entries):
                return self.entries[row_id]
            else:
                raise IndexError(f"Entry id {row_id} not in internal page {self.page_id}")
            
    #delete a value at a given row id (keep the row but make value contents null)
    def delete_value(self, row_id: int):
        # not going to support deletion for internal nodes
        if self.page_type != PageType.LEAF:
            raise NotImplementedError("Delete not supported for internal pages")

        #Check if this row id is even available (not deleted, not out of index)
        if row_id >= 0 and row_id < len(self.values) and row_id not in self.deleted_indices:
            # mark this page as modified
            self.dirty = True
            #record how much space will be freed up upon deletion
            deleted_value_size = len(self.values[row_id]) + 4 
            # nullify the value at row_id's location
            self.values[row_id] = None 
            # now decrement
            self.current_size -= deleted_value_size 
            #record that you deleted it
            self.deleted_indices.add(row_id) 
        else:
            raise IndexError(f"Row id {row_id} not on {self.page_id} or deleted")


    # Serialize a given page into a bytestream that can be pushed to memory
    def to_bytes(self) -> bytes:
        # initialize a byte stream which we will fill with the page's values after serialization
        byte_list = bytearray()

        # Add 1-byte page type at the beginning (0 = leaf, 1 = internal)
        byte_list.append(self.page_type.value)

        # LEAF flow
        if self.page_type == PageType.LEAF:
            # Loop through values and turn them to bytes with prefixes that specify their lengths
            for i, value in enumerate(self.values):
                # 0-length prefixes awarded to deleted rows (trade off decision, alternatives too complicated)
                if i in self.deleted_indices or value is None:
                    #send 0 to byte stream if values are null    
                    byte_list.extend((0).to_bytes(4, 'big')) 
                else:
                    #record how many values are coming
                    prefix = len(value) 
                    #send this prefix and values to bytestream
                    byte_list.extend(prefix.to_bytes(4, 'big'))
                    byte_list.extend(value)
        
        # INTERNAL flow
        else:
            # Look through entries and turn them to bytes with prefixes for key length
            for key, child_pid in self.entries:
                key_len = len(key)
                # Send 2 bytes for key length to bytearray
                byte_list.extend(key_len.to_bytes(2, 'big'))  
                byte_list.extend(key)
                # Send 4-byte child pointer to bytearray
                byte_list.extend(struct.pack(">I", child_pid))  
            

        # Create padding for pages that aren't full based on how much space is left out of max_size
        padding = self.max_size - len(byte_list)
        #Check if bytestream is more than a page in gdb (over 4KB)
        if padding < 0: 
            raise ValueError("Too much content for simpledb's page size")
        #fill whatever space is left in the bytestream up to 4KB with padding
        byte_list.extend(b'\x00' * padding) 
        return bytes(byte_list)

    # Load a page instance from a given bytestream
    def _load_from_bytes(self, raw: bytes):
        # start an index that shows us where in the bytestream we are and set reading cutoff limit
        i = 0
        self.page_type = PageType(raw[i])
        i+=1

        # LEAF flow
        if self.page_type == PageType.LEAF:
            self.values = []
            # loop contents of the bytestream (minimum 4 --> if next value = deleted row prefix)
            while i + 4 <= len(raw):
                #read prefix and assign it to length and move past it
                length_bytes = raw[i:i+4]
                length = int.from_bytes(length_bytes, 'big') 
                i += 4

                #if value == 0 then this is a deleted row (previous tradeoff decision)
                if length == 0:
                    #mark row as null and add it to deleted_indices
                    self.values.append(None)
                    self.deleted_indices.add(len(self.values) - 1)
                    continue
                # check if prefix+value is longer than bytestream (shouldn't be possible)
                if i + length > len(raw):
                    break  # Incomplete value — stop loading
                
                # identify value
                value = raw[i:i+length]
                # append value to page's values
                self.values.append(value)
                #increment size of page
                self.current_size += length + 4
                # move index
                i += length
        # INTERNAL flow
        else:  
            self.entries = []
            while i + 2 <= len(raw):
                key_len = int.from_bytes(raw[i:i+2], 'big')
                i += 2
                if i + key_len + 4 > len(raw):
                    break
                key = raw[i:i+key_len]
                i += key_len
                child_pid = struct.unpack(">I", raw[i:i+4])[0]
                i += 4
                self.entries.append((key, child_pid))
                self.current_size += 4 + key_len + 2


        #mark this page as unmodified as its justy loaded from disk    
        self.dirty = False

    # Create a page from raw bytes leveraging the init and _load_from_bytes methods
    @staticmethod
    def from_bytes(page_id: int, raw: bytes, max_size: int = PAGE_SIZE) -> "Page":
        page_type = PageType(raw[0])
        return Page(page_id=page_id, max_size=max_size, data=raw, page_type=page_type)
    

    def __repr__(self):
        if self.page_type == PageType.LEAF:
            return f"<LeafPage id={self.page_id} values={len(self.values)} dirty={self.dirty}>"
        else:
            return f"<InternalPage id={self.page_id} entries={len(self.entries)} dirty={self.dirty}>"
