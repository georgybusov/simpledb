import os
from constants import PAGE_SIZE
from page import Page, PageType  # Import your Page class
from file_handler import FileHandler

class Pager:
    def __init__(self, file_path, max_cache_size=100 * PAGE_SIZE):      
        # use file_handler to work with the file
        self.file_handler = FileHandler(file_path)
        self.max_cache_size = max_cache_size
        self.cache = {}
        # track which pages need to be written to disk due to modification
        self.dirty_pages = set()
        # track number of pages
        self.num_pages = self.file_handler.file_size // PAGE_SIZE  

    # retrieve a Page instance from disk or cache given its location
    def get_page(self, page_id):
        # return from memory if cached
        if page_id in self.cache:
            return self.cache[page_id]
        
        # Calculate byte offset in file
        offset = page_id * PAGE_SIZE
        file_size = self.file_handler.file_size

        # If the page does NOT exist yet, create new
        if offset >= file_size:
            # Make a new blank leaf page
            page = Page(page_id=page_id, page_type=PageType.LEAF)    
        

        else:
            # else read and deserialize existing page 
            raw_data = self.file_handler.read_bytes(offset, PAGE_SIZE)

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

            self.file_handler.write_bytes(offset, serialized)

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
        self.file_handler.close()
