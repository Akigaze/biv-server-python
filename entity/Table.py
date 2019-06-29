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
