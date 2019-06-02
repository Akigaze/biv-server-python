class __FileCache(object):
    def __init__(self):
        self.cache = dict()

    def add_file(self, file_id, file):
        self.cache[file_id] = file

    def clear_cache(self):
        self.cache = dict()

    def get_file_ids(self):
        return [key for key in self.cache.keys()]

    def get_file_by_id(self, file_id):
        return self.cache.get(file_id)


file_cache = __FileCache()

