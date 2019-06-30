import json


class Table(object):
    def __init__(self, name, fields, row_count):
        self.name = name
        self.fields = fields
        self.row_count = row_count

    def to_json(self):
        fields_list = [field.to_json() for field in self.fields]
        table_dict = {
            "name": self.name,
            "fields": fields_list,
            "count": self.row_count
        }
        return json.dumps(table_dict)


class Field(object):
    def __init__(self, id, ctype, name_of_sheet=None, name_of_db=None):
        self.id = id
        self.name_of_sheet = name_of_sheet
        self.name_of_db = name_of_db.replace(r'\s+', "_")
        self.ctype = ctype

    def to_json(self):
        return{
            "id": self.id,
            "nameOfSheet": self.name_of_sheet,
            "nameOfDatabase": self.name_of_db,
            "type": self.ctype,
        }


class Property(object):
    ID = "id"
    NAME_OF_SHEET = "nameOfSheet"
    NAME_OF_DB = "nameOfDatabase"
    CTYPE = "type"
    NAME = "name"
