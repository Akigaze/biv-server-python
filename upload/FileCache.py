class FileCache(object):
    def __init__(self):
        self.cache = dict()

    def add_file(self, file_id, file):
        self.cache[file_id] = file

    def clear_cache(self):
        self.cache = dict()
