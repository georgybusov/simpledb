import os
from constants import PAGE_SIZE
from page import Page  # Import your Page class



class Pager:
    def __init__(self, file_path, max_cache_size=100 * PAGE_SIZE):      
        self.file_path = file_path
        self.max_cache_size = max_cache_size
        self.cache = {}
        # track which pages need to be written to disk due to modification
        self.dirty_pages = set()

        # open the file in read/write mode, create if it doesn't exist
        if not os.path.exists(self.file_path):
            self.file = open(self.file_path, 'w+b')
        else:
            self.file = open(self.file_path, 'r+b')

        # seek to the end of the file to determine how many full pages exist
        self.file.seek(0, os.SEEK_END)
        # total pages on disk
        self.num_pages = self.file.tell() // PAGE_SIZE  

    # retrieve a Page instance from disk or cache given its location
    def get_page(self, page_id):
        # return from memory if cached
        if page_id in self.cache:
            return self.cache[page_id]
        # else we need to read from disk
        offset = page_id * PAGE_SIZE
        self.file.seek(offset)
        raw_data = self.file.read(PAGE_SIZE)
        # Create a Page object from the raw data
        page = Page.from_bytes(page_id=page_id, raw=raw_data)
        # Store in cache and return
        self.cache[page_id] = page
        return page

    def mark_dirty(self, page_id):
        # dirty pages have been modified in cache
        self.dirty_pages.add(page_id)

    def write_page(self, page_id):
        # Serialize and write a page to disk if it's marked as dirty.
        # Get the page from cache or load it from disk if not present
        page = self.cache.get(page_id) or self.get_page(page_id)

        # Only write if it's dirty
        if page_id in self.dirty_pages:
            serialized = page.to_bytes()
            offset = page_id * PAGE_SIZE

            self.file.seek(offset)
            self.file.write(serialized)
            self.file.flush()  # Ensure bytes are actually written to disk

            self.dirty_pages.remove(page_id)

    def flush_all(self):

        # Write all dirty pages in cache to disk
        for page_id in list(self.dirty_pages):
            self.write_page(page_id)

    def allocate_new_page(self, page_type):
        # Create a new empty page and assign the next available page id
        page_id = self.num_pages  # Next free page index
        page = Page(page_id=page_id, page_type=page_type)
        self.cache[page_id] = page
        self.dirty_pages.add(page_id)
        self.num_pages += 1
        return page

    def close(self):
        # write all dirty pages to disk and close the file
        self.flush_all()
        self.file.close()
