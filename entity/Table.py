import json


class Table(object):
    def __init__(self, name, fields):
        self.name = name
        self.fields = fields

    def to_json(self):
        fields_list = [field.__dict__ for field in self.fields]
        table_dict = {
            "name": self.name,
            "fields": fields_list
        }
        return json.dumps(table_dict)
