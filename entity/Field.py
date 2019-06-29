class Field(object):
    def __init__(self, id, ctype, name_of_sheet=None, name_of_db=None):
        self.id = id
        self.name_of_sheet = name_of_sheet
        self.name_of_db = name_of_db.replace(r'\s+', "_")
        self.ctype = ctype

    def to_json(self):
        json = {
            "id": self.id,
            "nameOfSheet": self.name_of_sheet,
            "nameOfDatabase": self.name_of_db,
            "type": self.ctype,
        }
        return json


class Property(object):
    ID = "id"
    NAME_OF_SHEET = "nameOfSheet"
    NAME_OF_DB = "nameOfDatabase"
    CTYPE = "type"
    NAME = "name"
