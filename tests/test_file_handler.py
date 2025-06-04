import os
from file_handler import FileHandler


def test_file_handler():
    test_path = "example.db"

    if os.path.exists(test_path):
        os.remove(test_path)

    handler = FileHandler(test_path)

    with open("example.db","wb") as f:
        f.write(b'420')
        f.close()

    with open("example.db",'rb') as f:
        read_data = f.read()
        f.close()
    # read_bytes should work same as f.read()
    assert read_data == handler.read_bytes(0,9999) , "Failed to read bytes"

    # write at specific offset
    handler.write_bytes(0, b'420')
    assert handler.read_bytes(0, 3) == b'420', "Failed to read written bytes"
    assert handler.file_size == 3 , "file size not added correctly"

    # append to a file
    handler.append_bytes(b'69')

    assert handler.read_bytes(0,9999) == b'42069', "Failed to append bytes"

    handler.close()

    # reset test
    os.remove(test_path)
    print(f"FileHandler class tests passed")