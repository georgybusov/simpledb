# Key Responsibilities:
    # Opening and closing the database file.
    # Reading a specified number of bytes from a given offset.
    # Writing a specified number of bytes to a given offset.
    # Appending bytes to the end of the file.
    # Reporting the current size of the file.

import os

class FileHandler:
    
    def __init__(self,file_path):
        self.file_path = file_path

        # create the file in the init call so that you can refer to it later. Everytime you instantiate FileHandler you will be working with an open file anyway
        if not os.path.exists(self.file_path):
            self.file = open(file_path,'w+b')
        else:
            self.file = open(file_path, 'r+b')
      
    def close(self):
        # FileHanlder.close()
        self.file.close()

    # given a specific offset, write bytes to that offset in the file
    def write_bytes(self,offset:int,bytes_to_write:bytes):
        self.file.seek(offset)
        self.file.write(bytes_to_write)
        self.file.flush()

    # given a specific offset, read from that offset in the file
    def read_bytes(self,offset:int,number_of_bytes_to_read:int):
        self.file.seek(offset)
        read_bytes = self.file.read(number_of_bytes_to_read)
        return read_bytes

    def append_bytes(self,bytes_to_append:bytes):
        self.file.seek(0,os.SEEK_END)
        self.file.write(bytes_to_append)
        self.file.flush()
    
    @property
    def file_size(self):
        self.file.seek(0,os.SEEK_END)
        return self.file.tell()




